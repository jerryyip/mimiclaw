#include "voice/voice_channel.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include "mimi_config.h"
#include "bus/message_bus.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

#include "esp_log.h"
#include "esp_http_client.h"
#include "esp_crt_bundle.h"
#include "esp_heap_caps.h"
#include "driver/i2s_std.h"

#include "cJSON.h"
#include "mbedtls/base64.h"

static const char *TAG = "voice";

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} http_resp_t;

static bool s_enabled = false;
static bool s_i2s_ready = false;
static volatile bool s_is_playing = false;

static i2s_chan_handle_t s_tx_chan = NULL;
static i2s_chan_handle_t s_rx_chan = NULL;

static TaskHandle_t s_capture_task = NULL;
static SemaphoreHandle_t s_http_lock = NULL;

static esp_err_t http_event_handler(esp_http_client_event_t *evt)
{
    http_resp_t *resp = (http_resp_t *)evt->user_data;
    if (evt->event_id != HTTP_EVENT_ON_DATA || !resp) {
        return ESP_OK;
    }

    if (resp->len + evt->data_len + 1 > resp->cap) {
        size_t new_cap = resp->cap ? resp->cap * 2 : 4096;
        size_t needed = resp->len + evt->data_len + 1;
        if (new_cap < needed) {
            new_cap = needed;
        }
        char *tmp = realloc(resp->buf, new_cap);
        if (!tmp) {
            return ESP_ERR_NO_MEM;
        }
        resp->buf = tmp;
        resp->cap = new_cap;
    }

    memcpy(resp->buf + resp->len, evt->data, evt->data_len);
    resp->len += evt->data_len;
    resp->buf[resp->len] = '\0';
    return ESP_OK;
}

static esp_err_t i2s_init_xvf3800(void)
{
    if (MIMI_VOICE_I2S_BCLK < 0 || MIMI_VOICE_I2S_WS < 0 ||
        MIMI_VOICE_I2S_DIN < 0 || MIMI_VOICE_I2S_DOUT < 0) {
        ESP_LOGW(TAG, "Voice disabled: configure I2S pins in mimi_secrets.h");
        return ESP_ERR_INVALID_STATE;
    }

    i2s_chan_config_t chan_cfg = I2S_CHANNEL_DEFAULT_CONFIG((i2s_port_t)MIMI_VOICE_I2S_PORT,
                                                             I2S_ROLE_MASTER);
    esp_err_t err = i2s_new_channel(&chan_cfg, &s_tx_chan, &s_rx_chan);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "i2s_new_channel failed: %s", esp_err_to_name(err));
        return err;
    }

    i2s_std_config_t std_cfg = {
        .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(MIMI_VOICE_SAMPLE_RATE),
        .slot_cfg = I2S_STD_MSB_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_MONO),
        .gpio_cfg = {
            .mclk = I2S_GPIO_UNUSED,
            .bclk = MIMI_VOICE_I2S_BCLK,
            .ws = MIMI_VOICE_I2S_WS,
            .dout = MIMI_VOICE_I2S_DOUT,
            .din = MIMI_VOICE_I2S_DIN,
            .invert_flags = {
                .mclk_inv = false,
                .bclk_inv = false,
                .ws_inv = false,
            },
        },
    };

    err = i2s_channel_init_std_mode(s_rx_chan, &std_cfg);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "i2s_channel_init_std_mode(rx) failed: %s", esp_err_to_name(err));
        return err;
    }
    err = i2s_channel_init_std_mode(s_tx_chan, &std_cfg);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "i2s_channel_init_std_mode(tx) failed: %s", esp_err_to_name(err));
        return err;
    }

    ESP_ERROR_CHECK(i2s_channel_enable(s_rx_chan));
    ESP_ERROR_CHECK(i2s_channel_enable(s_tx_chan));

    s_i2s_ready = true;
    ESP_LOGI(TAG, "I2S ready for XVF3800: %d Hz mono s16", MIMI_VOICE_SAMPLE_RATE);
    return ESP_OK;
}

static int frame_avg_abs_energy(const int16_t *samples, size_t sample_count)
{
    if (!samples || sample_count == 0) {
        return 0;
    }

    uint64_t sum = 0;
    for (size_t i = 0; i < sample_count; i++) {
        int v = samples[i];
        if (v < 0) {
            v = -v;
        }
        sum += (uint32_t)v;
    }
    return (int)(sum / sample_count);
}

static const char *stt_api_url(void)
{
    return MIMI_SECRET_STT_URL[0] ? MIMI_SECRET_STT_URL : MIMI_QWEN_STT_URL;
}

static const char *stt_model(void)
{
    return MIMI_SECRET_STT_MODEL[0] ? MIMI_SECRET_STT_MODEL : MIMI_QWEN_STT_MODEL;
}

static const char *tts_api_url(void)
{
    return MIMI_SECRET_TTS_URL[0] ? MIMI_SECRET_TTS_URL : MIMI_QWEN_TTS_URL;
}

static const char *tts_model(void)
{
    return MIMI_SECRET_TTS_MODEL[0] ? MIMI_SECRET_TTS_MODEL : MIMI_QWEN_TTS_MODEL;
}

static const char *stt_api_key(void)
{
    if (MIMI_SECRET_STT_API_KEY[0]) {
        return MIMI_SECRET_STT_API_KEY;
    }
    return MIMI_SECRET_API_KEY;
}

static const char *tts_api_key(void)
{
    if (MIMI_SECRET_TTS_API_KEY[0]) {
        return MIMI_SECRET_TTS_API_KEY;
    }
    return MIMI_SECRET_API_KEY;
}

static esp_err_t http_post_json(const char *url, const char *api_key, const char *json,
                                const char *accept, char **out_body, int *out_status)
{
    if (!url || !json || !out_body || !out_status) {
        return ESP_ERR_INVALID_ARG;
    }
    *out_body = NULL;
    *out_status = 0;

    http_resp_t resp = {
        .buf = calloc(1, 4096),
        .len = 0,
        .cap = 4096,
    };
    if (!resp.buf) {
        return ESP_ERR_NO_MEM;
    }

    esp_http_client_config_t cfg = {
        .url = url,
        .event_handler = http_event_handler,
        .user_data = &resp,
        .timeout_ms = 30000,
        .buffer_size = 2048,
        .buffer_size_tx = 2048,
        .crt_bundle_attach = esp_crt_bundle_attach,
    };
    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    if (!client) {
        free(resp.buf);
        return ESP_FAIL;
    }

    esp_http_client_set_method(client, HTTP_METHOD_POST);
    esp_http_client_set_header(client, "Content-Type", "application/json");
    if (accept && accept[0]) {
        esp_http_client_set_header(client, "Accept", accept);
    }
    if (api_key && api_key[0]) {
        char auth[256];
        snprintf(auth, sizeof(auth), "Bearer %s", api_key);
        esp_http_client_set_header(client, "Authorization", auth);
    }
    esp_http_client_set_header(client, "X-DashScope-SSE", "disable");

    esp_http_client_set_post_field(client, json, (int)strlen(json));
    esp_err_t err = esp_http_client_perform(client);
    *out_status = esp_http_client_get_status_code(client);
    esp_http_client_cleanup(client);
    if (err != ESP_OK) {
        free(resp.buf);
        return err;
    }

    *out_body = resp.buf;
    return ESP_OK;
}

static esp_err_t i2s_write_all(const uint8_t *data, size_t len)
{
    if (!data || len == 0) {
        return ESP_OK;
    }

    size_t off = 0;
    while (off < len) {
        size_t written = 0;
        size_t chunk = len - off;
        if (chunk > 1024) {
            chunk = 1024;
        }
        esp_err_t err = i2s_channel_write(s_tx_chan, data + off, chunk, &written,
                                          pdMS_TO_TICKS(500));
        if (err != ESP_OK) {
            return err;
        }
        off += written;
    }
    return ESP_OK;
}

static uint32_t read_le_u32(const uint8_t *p)
{
    return ((uint32_t)p[0]) | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint16_t read_le_u16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0]) | ((uint16_t)p[1] << 8));
}

static esp_err_t wav_find_data_offset(const uint8_t *wav, size_t wav_len,
                                      size_t *out_data_off, uint32_t *out_sample_rate)
{
    if (!wav || wav_len < 12 || !out_data_off || !out_sample_rate) {
        return ESP_ERR_INVALID_ARG;
    }
    *out_data_off = 0;
    *out_sample_rate = 0;

    if (memcmp(wav, "RIFF", 4) != 0 || memcmp(wav + 8, "WAVE", 4) != 0) {
        return ESP_ERR_INVALID_RESPONSE;
    }

    bool fmt_ok = false;
    size_t off = 12;
    while (off + 8 <= wav_len) {
        const uint8_t *chunk = wav + off;
        uint32_t chunk_size = read_le_u32(chunk + 4);
        size_t payload_off = off + 8;
        size_t next = payload_off + chunk_size + (chunk_size & 1u);
        if (payload_off > wav_len || next > wav_len) {
            return ESP_ERR_NOT_FOUND;
        }

        if (memcmp(chunk, "fmt ", 4) == 0 && chunk_size >= 16) {
            const uint8_t *f = wav + payload_off;
            uint16_t audio_format = read_le_u16(f + 0);
            uint16_t channels = read_le_u16(f + 2);
            *out_sample_rate = read_le_u32(f + 4);
            uint16_t bits_per_sample = read_le_u16(f + 14);
            if (audio_format == 1 && channels == 1 && bits_per_sample == 16) {
                fmt_ok = true;
            }
        } else if (memcmp(chunk, "data", 4) == 0) {
            if (!fmt_ok) {
                return ESP_ERR_INVALID_RESPONSE;
            }
            *out_data_off = payload_off;
            return ESP_OK;
        }
        off = next;
    }

    return ESP_ERR_NOT_FOUND;
}

static esp_err_t build_wav_from_pcm(const int16_t *pcm, size_t pcm_bytes,
                                    uint8_t **out_wav, size_t *out_wav_len)
{
    if (!pcm || !out_wav || !out_wav_len) {
        return ESP_ERR_INVALID_ARG;
    }
    if (pcm_bytes > UINT32_MAX - 44) {
        return ESP_ERR_INVALID_SIZE;
    }

    uint8_t *wav = malloc(44 + pcm_bytes);
    if (!wav) {
        return ESP_ERR_NO_MEM;
    }

    uint32_t data_size = (uint32_t)pcm_bytes;
    uint32_t riff_size = 36 + data_size;
    uint32_t byte_rate = MIMI_VOICE_SAMPLE_RATE * 1 * (MIMI_VOICE_BITS_PER_SAMPLE / 8);
    uint16_t block_align = 1 * (MIMI_VOICE_BITS_PER_SAMPLE / 8);

    memcpy(wav + 0, "RIFF", 4);
    wav[4] = (uint8_t)(riff_size & 0xFF);
    wav[5] = (uint8_t)((riff_size >> 8) & 0xFF);
    wav[6] = (uint8_t)((riff_size >> 16) & 0xFF);
    wav[7] = (uint8_t)((riff_size >> 24) & 0xFF);
    memcpy(wav + 8, "WAVE", 4);
    memcpy(wav + 12, "fmt ", 4);
    wav[16] = 16; wav[17] = 0; wav[18] = 0; wav[19] = 0;
    wav[20] = 1; wav[21] = 0;
    wav[22] = 1; wav[23] = 0;
    wav[24] = (uint8_t)(MIMI_VOICE_SAMPLE_RATE & 0xFF);
    wav[25] = (uint8_t)((MIMI_VOICE_SAMPLE_RATE >> 8) & 0xFF);
    wav[26] = (uint8_t)((MIMI_VOICE_SAMPLE_RATE >> 16) & 0xFF);
    wav[27] = (uint8_t)((MIMI_VOICE_SAMPLE_RATE >> 24) & 0xFF);
    wav[28] = (uint8_t)(byte_rate & 0xFF);
    wav[29] = (uint8_t)((byte_rate >> 8) & 0xFF);
    wav[30] = (uint8_t)((byte_rate >> 16) & 0xFF);
    wav[31] = (uint8_t)((byte_rate >> 24) & 0xFF);
    wav[32] = (uint8_t)(block_align & 0xFF);
    wav[33] = (uint8_t)((block_align >> 8) & 0xFF);
    wav[34] = MIMI_VOICE_BITS_PER_SAMPLE;
    wav[35] = 0;
    memcpy(wav + 36, "data", 4);
    wav[40] = (uint8_t)(data_size & 0xFF);
    wav[41] = (uint8_t)((data_size >> 8) & 0xFF);
    wav[42] = (uint8_t)((data_size >> 16) & 0xFF);
    wav[43] = (uint8_t)((data_size >> 24) & 0xFF);
    memcpy(wav + 44, pcm, pcm_bytes);

    *out_wav = wav;
    *out_wav_len = 44 + pcm_bytes;
    return ESP_OK;
}

static esp_err_t base64_encode_alloc(const uint8_t *src, size_t src_len, char **out_b64)
{
    if (!src || !out_b64) {
        return ESP_ERR_INVALID_ARG;
    }
    *out_b64 = NULL;

    size_t needed = 0;
    int rc = mbedtls_base64_encode(NULL, 0, &needed, src, src_len);
    if (rc != MBEDTLS_ERR_BASE64_BUFFER_TOO_SMALL || needed == 0) {
        return ESP_FAIL;
    }

    char *b64 = malloc(needed + 1);
    if (!b64) {
        return ESP_ERR_NO_MEM;
    }
    rc = mbedtls_base64_encode((unsigned char *)b64, needed, &needed, src, src_len);
    if (rc != 0) {
        free(b64);
        return ESP_FAIL;
    }
    b64[needed] = '\0';
    *out_b64 = b64;
    return ESP_OK;
}

static esp_err_t parse_qwen_asr_text(const char *json, char *out_text, size_t out_size)
{
    cJSON *root = cJSON_Parse(json);
    if (!root) {
        return ESP_ERR_INVALID_RESPONSE;
    }

    const char *text = NULL;
    cJSON *choices = cJSON_GetObjectItem(root, "choices");
    if (cJSON_IsArray(choices)) {
        cJSON *first = cJSON_GetArrayItem(choices, 0);
        cJSON *message = cJSON_GetObjectItem(first, "message");
        cJSON *content = cJSON_GetObjectItem(message, "content");
        if (cJSON_IsString(content) && content->valuestring) {
            text = content->valuestring;
        } else if (cJSON_IsArray(content)) {
            cJSON *item = cJSON_GetArrayItem(content, 0);
            cJSON *item_text = cJSON_GetObjectItem(item, "text");
            if (cJSON_IsString(item_text) && item_text->valuestring) {
                text = item_text->valuestring;
            }
        }
    }

    if (!text) {
        cJSON *output = cJSON_GetObjectItem(root, "output");
        cJSON *result = output ? cJSON_GetObjectItem(output, "text") : NULL;
        if (cJSON_IsString(result) && result->valuestring) {
            text = result->valuestring;
        }
    }

    if (text && out_size > 0) {
        strncpy(out_text, text, out_size - 1);
        out_text[out_size - 1] = '\0';
    }
    cJSON_Delete(root);
    return (text && out_text[0]) ? ESP_OK : ESP_ERR_NOT_FOUND;
}

static esp_err_t stt_transcribe_pcm(const int16_t *pcm, size_t bytes,
                                    char *out_text, size_t out_text_size)
{
    if (!out_text || out_text_size == 0) {
        return ESP_ERR_INVALID_ARG;
    }
    out_text[0] = '\0';

    const char *url = stt_api_url();
    const char *api_key = stt_api_key();
    if (!url[0] || !api_key[0]) {
        return ESP_ERR_INVALID_STATE;
    }

    uint8_t *wav = NULL;
    size_t wav_len = 0;
    char *audio_b64 = NULL;
    char *data_uri = NULL;
    char *json = NULL;
    char *resp_body = NULL;
    int status = 0;
    esp_err_t err = ESP_FAIL;

    err = build_wav_from_pcm(pcm, bytes, &wav, &wav_len);
    if (err != ESP_OK) goto done;
    err = base64_encode_alloc(wav, wav_len, &audio_b64);
    if (err != ESP_OK) goto done;

    const char *prefix = "data:audio/wav;base64,";
    data_uri = malloc(strlen(prefix) + strlen(audio_b64) + 1);
    if (!data_uri) {
        err = ESP_ERR_NO_MEM;
        goto done;
    }
    strcpy(data_uri, prefix);
    strcat(data_uri, audio_b64);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "model", stt_model());
    cJSON_AddBoolToObject(root, "stream", false);

    cJSON *modalities = cJSON_CreateArray();
    cJSON_AddItemToArray(modalities, cJSON_CreateString("text"));
    cJSON_AddItemToObject(root, "modalities", modalities);

    cJSON *messages = cJSON_CreateArray();
    cJSON *msg = cJSON_CreateObject();
    cJSON_AddStringToObject(msg, "role", "user");
    cJSON *content = cJSON_CreateArray();
    cJSON *audio_item = cJSON_CreateObject();
    cJSON_AddStringToObject(audio_item, "type", "input_audio");
    cJSON *audio_obj = cJSON_CreateObject();
    cJSON_AddStringToObject(audio_obj, "data", data_uri);
    cJSON_AddStringToObject(audio_obj, "format", "wav");
    cJSON_AddItemToObject(audio_item, "input_audio", audio_obj);
    cJSON_AddItemToArray(content, audio_item);

    cJSON *text_item = cJSON_CreateObject();
    cJSON_AddStringToObject(text_item, "type", "text");
    cJSON_AddStringToObject(text_item, "text", "Transcribe this speech to plain text. Return only the transcript.");
    cJSON_AddItemToArray(content, text_item);
    cJSON_AddItemToObject(msg, "content", content);
    cJSON_AddItemToArray(messages, msg);
    cJSON_AddItemToObject(root, "messages", messages);

    json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) {
        err = ESP_ERR_NO_MEM;
        goto done;
    }

    err = http_post_json(url, api_key, json, "application/json", &resp_body, &status);
    if (err != ESP_OK) goto done;
    if (status < 200 || status >= 300) {
        ESP_LOGE(TAG, "Qwen STT HTTP %d: %.240s", status, resp_body);
        err = ESP_FAIL;
        goto done;
    }

    err = parse_qwen_asr_text(resp_body, out_text, out_text_size);

done:
    free(wav);
    free(audio_b64);
    free(data_uri);
    free(json);
    free(resp_body);
    return err;
}

static esp_err_t tts_stream_play(const char *text)
{
    char *json = NULL;
    char *resp_body = NULL;
    esp_http_client_handle_t client = NULL;
    uint8_t *prefix = NULL;
    esp_err_t err = ESP_FAIL;

    const char *url = tts_api_url();
    const char *api_key = tts_api_key();
    if (!url[0] || !api_key[0]) {
        return ESP_ERR_INVALID_STATE;
    }

    /* Step 1: request TTS task and get downloadable audio URL */
    cJSON *body = cJSON_CreateObject();
    if (!body) {
        return ESP_ERR_NO_MEM;
    }
    cJSON_AddStringToObject(body, "model", tts_model());
    cJSON *input = cJSON_CreateObject();
    cJSON_AddStringToObject(input, "text", text);
    cJSON_AddItemToObject(body, "input", input);
    cJSON *parameters = cJSON_CreateObject();
    cJSON_AddStringToObject(parameters, "voice", MIMI_SECRET_TTS_VOICE);
    cJSON_AddStringToObject(parameters, "language_type", MIMI_SECRET_TTS_LANGUAGE);
    cJSON_AddItemToObject(body, "parameters", parameters);
    json = cJSON_PrintUnformatted(body);
    cJSON_Delete(body);
    if (!json) {
        return ESP_ERR_NO_MEM;
    }

    int status = 0;
    err = http_post_json(url, api_key, json, "application/json", &resp_body, &status);
    free(json);
    json = NULL;
    if (err != ESP_OK || status < 200 || status >= 300) {
        ESP_LOGE(TAG, "Qwen TTS HTTP %d: %.240s", status, resp_body ? resp_body : "");
        goto done;
    }

    cJSON *root = cJSON_Parse(resp_body);
    free(resp_body);
    resp_body = NULL;
    if (!root) {
        err = ESP_ERR_INVALID_RESPONSE;
        goto done;
    }
    cJSON *output = cJSON_GetObjectItem(root, "output");
    cJSON *audio = output ? cJSON_GetObjectItem(output, "audio") : NULL;
    cJSON *url_obj = audio ? cJSON_GetObjectItem(audio, "url") : NULL;
    const char *audio_url = (cJSON_IsString(url_obj) && url_obj->valuestring) ? url_obj->valuestring : NULL;
    if (!audio_url) {
        cJSON_Delete(root);
        err = ESP_ERR_INVALID_RESPONSE;
        goto done;
    }

    esp_http_client_config_t cfg = {
        .url = audio_url,
        .timeout_ms = 30000,
        .buffer_size = 2048,
        .buffer_size_tx = 1024,
        .crt_bundle_attach = esp_crt_bundle_attach,
    };
    client = esp_http_client_init(&cfg);
    cJSON_Delete(root);
    if (!client) {
        err = ESP_FAIL;
        goto done;
    }

    esp_http_client_set_method(client, HTTP_METHOD_GET);
    esp_http_client_set_header(client, "Accept", "audio/wav,application/octet-stream");
    err = esp_http_client_open(client, 0);
    if (err != ESP_OK) {
        goto done;
    }
    status = esp_http_client_fetch_headers(client);
    (void)status;
    int code = esp_http_client_get_status_code(client);
    if (code < 200 || code >= 300) {
        err = ESP_FAIL;
        goto done;
    }

    /* Step 2: stream download and play WAV body */
    size_t prefix_cap = 4096;
    prefix = malloc(prefix_cap);
    if (!prefix) {
        err = ESP_ERR_NO_MEM;
        goto done;
    }
    size_t prefix_len = 0;
    bool data_started = false;

    s_is_playing = true;

    uint8_t chunk[2048];
    while (1) {
        int n = esp_http_client_read(client, (char *)chunk, sizeof(chunk));
        if (n < 0) {
            err = ESP_FAIL;
            break;
        }
        if (n == 0) {
            err = ESP_OK;
            break;
        }

        if (!data_started) {
            if (prefix_len + (size_t)n > prefix_cap) {
                size_t new_cap = prefix_cap * 2;
                while (new_cap < prefix_len + (size_t)n) {
                    new_cap *= 2;
                }
                uint8_t *tmp = realloc(prefix, new_cap);
                if (!tmp) {
                    err = ESP_ERR_NO_MEM;
                    break;
                }
                prefix = tmp;
                prefix_cap = new_cap;
            }
            memcpy(prefix + prefix_len, chunk, (size_t)n);
            prefix_len += (size_t)n;

            size_t data_off = 0;
            uint32_t sample_rate = 0;
            esp_err_t parse = wav_find_data_offset(prefix, prefix_len, &data_off, &sample_rate);
            if (parse == ESP_OK) {
                if (sample_rate != MIMI_VOICE_SAMPLE_RATE) {
                    ESP_LOGW(TAG, "TTS WAV sample rate=%u differs from I2S=%d; playback speed may be off",
                             sample_rate, MIMI_VOICE_SAMPLE_RATE);
                }
                if (prefix_len > data_off) {
                    err = i2s_write_all(prefix + data_off, prefix_len - data_off);
                    if (err != ESP_OK) {
                        break;
                    }
                }
                data_started = true;
            } else if (parse != ESP_ERR_NOT_FOUND) {
                err = parse;
                break;
            }
        } else {
            err = i2s_write_all(chunk, (size_t)n);
            if (err != ESP_OK) {
                break;
            }
        }
    }

done:
    s_is_playing = false;
    if (client) {
        esp_http_client_close(client);
        esp_http_client_cleanup(client);
    }
    free(prefix);
    free(resp_body);
    free(json);
    return err;
}

static void push_voice_inbound(const char *text)
{
    mimi_msg_t msg = {0};
    strncpy(msg.channel, MIMI_CHAN_VOICE, sizeof(msg.channel) - 1);
    strncpy(msg.chat_id, MIMI_VOICE_CHAT_ID, sizeof(msg.chat_id) - 1);
    msg.content = strdup(text);
    if (!msg.content) {
        return;
    }

    if (message_bus_push_inbound(&msg) != ESP_OK) {
        ESP_LOGW(TAG, "Inbound queue full, drop voice text");
        free(msg.content);
    }
}

static void voice_capture_task(void *arg)
{
    const size_t samples_per_frame = (MIMI_VOICE_SAMPLE_RATE * MIMI_VOICE_FRAME_MS) / 1000;
    const size_t frame_bytes = samples_per_frame * sizeof(int16_t);
    const int max_frames = MIMI_VOICE_MAX_UTTERANCE_MS / MIMI_VOICE_FRAME_MS;
    const int end_silence_frames = MIMI_VOICE_SILENCE_END_MS / MIMI_VOICE_FRAME_MS;

    int16_t *frame = malloc(frame_bytes);
    int16_t *utterance = heap_caps_malloc(max_frames * frame_bytes, MALLOC_CAP_SPIRAM);
    if (!utterance) {
        utterance = malloc(max_frames * frame_bytes);
    }

    if (!frame || !utterance) {
        free(frame);
        free(utterance);
        ESP_LOGE(TAG, "Cannot allocate voice capture buffers");
        vTaskDelete(NULL);
        return;
    }

    bool in_speech = false;
    int silence_frames = 0;
    int total_frames = 0;

    while (1) {
        if (s_is_playing) {
            vTaskDelay(pdMS_TO_TICKS(30));
            continue;
        }

        size_t read_bytes = 0;
        esp_err_t err = i2s_channel_read(s_rx_chan, frame, frame_bytes, &read_bytes,
                                         pdMS_TO_TICKS(200));
        if (err != ESP_OK || read_bytes != frame_bytes) {
            continue;
        }

        int energy = frame_avg_abs_energy(frame, samples_per_frame);
        bool voiced = (energy >= MIMI_VOICE_VAD_THRESHOLD);

        if (!in_speech) {
            if (!voiced) {
                continue;
            }
            in_speech = true;
            silence_frames = 0;
            total_frames = 0;
        }

        if (total_frames < max_frames) {
            memcpy((uint8_t *)utterance + (total_frames * frame_bytes), frame, frame_bytes);
            total_frames++;
        }

        if (voiced) {
            silence_frames = 0;
        } else {
            silence_frames++;
        }

        bool hit_max = (total_frames >= max_frames);
        bool end_of_speech = (silence_frames >= end_silence_frames);
        if (!hit_max && !end_of_speech) {
            continue;
        }

        in_speech = false;
        if (total_frames < 5) {
            continue;
        }

        size_t pcm_bytes = total_frames * frame_bytes;
        char text[512] = {0};

        if (xSemaphoreTake(s_http_lock, pdMS_TO_TICKS(30000)) == pdTRUE) {
            esp_err_t stt_err = stt_transcribe_pcm(utterance, pcm_bytes, text, sizeof(text));
            xSemaphoreGive(s_http_lock);
            if (stt_err == ESP_OK && text[0]) {
                ESP_LOGI(TAG, "Voice STT: %s", text);
                push_voice_inbound(text);
            } else {
                ESP_LOGW(TAG, "STT failed or empty transcript");
            }
        }
    }
}

esp_err_t voice_channel_init(void)
{
    s_enabled = (MIMI_VOICE_ENABLED_DEFAULT != 0) ||
                (stt_api_key()[0] && tts_api_key()[0]);
    if (!s_enabled) {
        ESP_LOGI(TAG, "Voice channel disabled (set STT/TTS API key or enable default)");
        return ESP_OK;
    }

    esp_err_t err = i2s_init_xvf3800();
    if (err != ESP_OK) {
        s_enabled = false;
        return ESP_OK;
    }

    s_http_lock = xSemaphoreCreateMutex();
    if (!s_http_lock) {
        ESP_LOGE(TAG, "Voice init failed: cannot allocate mutex");
        s_enabled = false;
        return ESP_ERR_NO_MEM;
    }

    return ESP_OK;
}

esp_err_t voice_channel_start(void)
{
    if (!s_enabled || !s_i2s_ready) {
        return ESP_OK;
    }

    if (!s_capture_task) {
        if (xTaskCreatePinnedToCore(voice_capture_task, "voice_cap",
                                    MIMI_VOICE_CAPTURE_STACK, NULL,
                                    MIMI_VOICE_TASK_PRIO, &s_capture_task,
                                    MIMI_VOICE_CORE) != pdPASS) {
            return ESP_FAIL;
        }
    }

    ESP_LOGI(TAG, "Voice channel started");
    return ESP_OK;
}

esp_err_t voice_channel_speak_text(const char *text)
{
    if (!s_enabled || !s_i2s_ready || !text || text[0] == '\0') {
        return ESP_ERR_INVALID_STATE;
    }

    if (xSemaphoreTake(s_http_lock, pdMS_TO_TICKS(30000)) != pdTRUE) {
        return ESP_ERR_TIMEOUT;
    }

    esp_err_t err = tts_stream_play(text);

    xSemaphoreGive(s_http_lock);
    return err;
}

bool voice_channel_is_enabled(void)
{
    return s_enabled;
}

void voice_channel_get_status(voice_channel_status_t *status)
{
    if (!status) {
        return;
    }
    status->enabled = s_enabled;
    status->i2s_ready = s_i2s_ready;
    status->is_playing = s_is_playing;
    status->stt_configured = (stt_api_url()[0] != '\0' && stt_api_key()[0] != '\0');
    status->tts_configured = (tts_api_url()[0] != '\0' && tts_api_key()[0] != '\0');
}
