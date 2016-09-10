'use strict'

importScripts('ende.js');

_init();

let ende_input = _get_input_buffer();
let ende_output = _get_output_buffer();
let broadcast_key = null;

onmessage = (evt) => {
    if (broadcast_key == null) {
        broadcast_key = evt.data;
        return;
    }

    let buffers = evt.data;
    let channels = buffers.length;
    let samples = buffers[0].length;
    let off = 0;
    for (let idx = 0; idx < samples; idx++) {
        for (let ch = 0; ch < channels; ch++) {
            HEAP32[ende_input / 4 + off] =
                Math.min(32767, Math.floor(buffers[ch][idx] * 32768));
            off++;
        }
    }

    let enclen = _encode(samples);
    let data = HEAP8.slice(ende_output, ende_output + enclen);

    let req = new XMLHttpRequest();
    req.onload = (evt) => {
        let resp = req.responseText;
        postMessage(resp);
    };
    req.open("POST", "/pushchunk/" + broadcast_key, "true");
    req.send(data.buffer);
};
