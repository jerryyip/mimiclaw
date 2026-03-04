#pragma once

#include <stdbool.h>
#include "esp_err.h"

typedef struct {
    bool enabled;
    bool i2s_ready;
    bool is_playing;
    bool stt_configured;
    bool tts_configured;
} voice_channel_status_t;

/*
 * Voice channel for ReSpeaker XVF3800 over I2S.
 *
 * Inbound path:
 *   Mic PCM -> VAD utterance -> STT -> message_bus inbound (channel=voice)
 *
 * Outbound path:
 *   Agent text (channel=voice) -> TTS -> speaker playback (I2S)
 */
esp_err_t voice_channel_init(void);
esp_err_t voice_channel_start(void);

/*
 * Convert text to speech and enqueue for playback.
 */
esp_err_t voice_channel_speak_text(const char *text);

bool voice_channel_is_enabled(void);
void voice_channel_get_status(voice_channel_status_t *status);
