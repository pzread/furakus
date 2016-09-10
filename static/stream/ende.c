#include<stdlib.h>
#include<stddef.h>
#include<string.h>
#include<assert.h>
#include<FLAC/stream_encoder.h>
#include<FLAC/stream_decoder.h>

#define INTERVAL 5
#define INPUT_MAXSIZE ((sizeof(FLAC__int32) * 2 * 44100 * INTERVAL) * 3 / 2)
#define OUTPUT_MAXSIZE INPUT_MAXSIZE

uint8_t *input_buffer;
uint8_t *output_buffer;
size_t output_offset;
size_t input_offset;
static FLAC__StreamEncoder *encoder;
static FLAC__StreamDecoder *decoder;

int init() {
    encoder = FLAC__stream_encoder_new();
    decoder = FLAC__stream_decoder_new();
    input_buffer = (uint8_t*)malloc(INPUT_MAXSIZE);
    assert(((uintptr_t)input_buffer & (sizeof(int32_t) - 1)) == 0);
    output_buffer = (uint8_t*)malloc(OUTPUT_MAXSIZE);
    assert(((uintptr_t)output_buffer & (sizeof(int32_t) - 1)) == 0);
    return 0;
}
uint8_t * get_input_buffer() {
    return input_buffer;
}
uint8_t * get_output_buffer() {
    return output_buffer;
}

FLAC__StreamEncoderWriteStatus enc_write_callback(
        const FLAC__StreamEncoder *encoder,
        const FLAC__byte buffer[],
        size_t bytes,
        unsigned samples,
        unsigned current_frame,
        void *client_data) {
    assert(output_offset + bytes <= OUTPUT_MAXSIZE);
    memcpy(output_buffer + output_offset, buffer, bytes);
    output_offset += bytes;
    return FLAC__STREAM_ENCODER_WRITE_STATUS_OK;
}

size_t encode(size_t samples) {
    output_offset = 0;
    FLAC__stream_encoder_init_stream(encoder, enc_write_callback,
            NULL, NULL, NULL, NULL);
    FLAC__stream_encoder_process_interleaved(encoder,
            (FLAC__int32*)input_buffer, samples);
    FLAC__stream_encoder_finish(encoder);
    return output_offset;
}

FLAC__StreamDecoderReadStatus dec_read_callback(
        const FLAC__StreamDecoder *decoder,
        FLAC__byte buffer[],
        size_t *bytes,
        void *client_data) {
    size_t *length = (size_t*)client_data;
    size_t len = *length - input_offset;
    if (len == 0) {
        *bytes = 0;
        return FLAC__STREAM_DECODER_READ_STATUS_END_OF_STREAM;
    }
    if (*bytes < len) {
        len = *bytes;
    }
    memcpy(buffer, input_buffer + input_offset, len);
    input_offset += len;
    *bytes = len;
    return FLAC__STREAM_DECODER_READ_STATUS_CONTINUE;
}

FLAC__StreamDecoderWriteStatus dec_write_callback(
        const FLAC__StreamDecoder *decoder,
        const FLAC__Frame *frame,
        const FLAC__int32 *const buffer[],
        void *client_data) {
    assert(frame->header.sample_rate == 44100);
    assert(frame->header.channels == 2);
    assert(frame->header.bits_per_sample == 16);
    unsigned len = frame->header.blocksize;
    assert(output_offset + (len * 2 * sizeof(FLAC__int32)) <= OUTPUT_MAXSIZE);
    for (unsigned idx = 0; idx < len; idx++) {
        for (unsigned ch = 0; ch < 2; ch++) {
            *(FLAC__int32*)(output_buffer + output_offset) = buffer[ch][idx];
            output_offset += sizeof(FLAC__int32);
        }
    }
    return FLAC__STREAM_DECODER_WRITE_STATUS_CONTINUE;
}

void dec_err_callback(
        const FLAC__StreamDecoder *decoder,
        FLAC__StreamDecoderErrorStatus status,
        void *client_data) {
    abort();
}

size_t decode(size_t length) {
    input_offset = 0;
    output_offset = 0;
    FLAC__stream_decoder_init_stream(decoder, dec_read_callback,
            NULL, NULL, NULL, NULL, dec_write_callback, NULL, dec_err_callback,
            &length);
    FLAC__stream_decoder_process_until_end_of_stream(decoder);
    FLAC__stream_decoder_finish(decoder);
    return output_offset;
}
