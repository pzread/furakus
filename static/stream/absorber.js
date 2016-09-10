'use strict'

importScripts('ende.js');

_init();

let ende_input = _get_input_buffer();
let ende_output = _get_output_buffer();
let token = null;

onmessage = (evt) => {
    if (token == null) {
        token = evt.data;
        return;
    }

    let index = evt.data;
    let req = new XMLHttpRequest();
    req.responseType = "arraybuffer";
    req.onload = (evt) => {
        if (req.status == 404) {
            let text = decodeURIComponent(escape(
                String.fromCharCode.apply(null, new Uint8Array(req.response))));
            postMessage(parseInt(text));
            return;
        }

        let resp = req.response;
        if (resp.length == 0) {
            postMessage(null);
            return;
        }

        let data = new Int8Array(resp);
        for (let idx = 0; idx < data.length; idx++) {
            HEAP8[ende_input + idx] = data[idx];
        }
        let declen = _decode(data.length);

        let channels = 2;
        let samples = declen / 4 / channels;
        let buffers = new Array(channels);

        for (let ch = 0; ch < channels; ch++) {
            buffers[ch] = new Float32Array(samples);
        }

        let off = 0;
        for (let idx = 0; idx < samples; idx++) {
            for (let ch = 0; ch < channels; ch++) {
                buffers[ch][idx] = HEAP32[ende_output / 4 + off] / 32768;
                off++;
            }
        }

        postMessage(buffers);
    };
    req.open("GET", "/pullchunk/" + token + "/" + index, "true");
    req.send();
}
