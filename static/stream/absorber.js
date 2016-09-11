'use strict'

importScripts('ende.js');

Module['_main'] = () => {
    _init();
    postMessage(null);
};

onmessage = (evt) => {
    let ende_input = _get_input_buffer();
    let ende_output = _get_output_buffer();
    let token = evt.data[0];
    let index = evt.data[1];
    let req = new XMLHttpRequest();
    req.responseType = "arraybuffer";
    req.onload = (evt) => {
        let resp = req.response;
        if (req.status == 404) {
            let text = decodeURIComponent(escape(
                String.fromCharCode.apply(null, new Uint8Array(resp))));
            if (text == '') {
                postMessage(null);
            } else {
                postMessage(parseInt(text));
            }
            return;
        }
        if (resp.length == 0) {
            postMessage(index + 1);
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
