'use strict'

let key = null;

onmessage = (evt) => {
    if (key == null) {
        key = evt.data;
        return;
    }

    let buffers = evt.data;
    let channels = buffers.length;
    let samples = buffers[0].length;
    let data = new ArrayBuffer(
        Int16Array.BYTES_PER_ELEMENT * samples * channels);
    let pcm = new Int16Array(data);
    let off = 0;
    for (let idx = 0; idx < samples; idx++) {
        for (let ch = 0; ch < channels; ch++) {
            pcm[off] = Math.min(32767, Math.floor(buffers[ch][idx] * 32768));
            off++;
        }
    }

    let req = new XMLHttpRequest();
    req.onload = (evt) => {
        let resp = req.responseText;
        postMessage(resp);
    };
    req.open("POST", "/pushchunk/" + key, "true");
    req.send(data);
};
