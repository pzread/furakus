'use strict'

let crypto = window.crypto || window.msCrypto;
let hasher = new Hashids();
let audio_ctx = new AudioContext();

function gen_token() {
    let rnd = new Uint8Array(3);
    crypto.getRandomValues(rnd);
    return hasher.encode(Array.from(rnd));
}

function main() {
    let parts = location.href.split('/');
    if (parts[parts.length - 1] == '') {
        let token = gen_token();
        $.post('/require/' + token + '/0').done((key) => {
            console.log(key);

            navigator.mediaDevices.getUserMedia({
                audio: {
                    mandatory: {
                        echoCancellation: false
                    },
                    optional: [{
                        echoCancellation: false
                    }]
                },
            }).then((stream) => {
                //let e_source = $('#source')[0];
                //let source_node = audio_ctx.createMediaElementSource(e_source);
                let source_node = audio_ctx.createMediaStreamSource(stream);
                let chunk_processor = new ChunkProcessor(audio_ctx, token, key);
                input(chunk_processor, source_node);

                $('#link').attr('href', token);
                $('#link').show();
                $('#run').show();
            })
        });
    } else {
        let token = parts[parts.length - 1];
        let chunk_processor = new ChunkProcessor(audio_ctx, token, null);
        output(chunk_processor);
    }
}

function input(chunk_processor, source_node) {
    let process_node = audio_ctx.createScriptProcessor(16384, 2, 2);
    process_node.onaudioprocess = (evt) => {
        chunk_processor.write_buffer(evt.inputBuffer);
    };

    /*let compressor_node = audio_ctx.createDynamicsCompressor();
    console.log(compressor_node);
    compressor_node.threshold.value = -50;
    compressor_node.knee.value = 40;
    compressor_node.ratio.value = 12;
    compressor_node.attack.value = 0;
    compressor_node.release.value = 0.25;
    compressor_node.connect(process_node);*/

    source_node.connect(process_node);
    process_node.connect(audio_ctx.destination);
}

function output(chunk_processor) {
    let process_node = audio_ctx.createScriptProcessor(16384, 2, 2);
    process_node.onaudioprocess = (evt) => {
        chunk_processor.read_buffer(evt.outputBuffer);
    };
    process_node.connect(audio_ctx.destination);
}

class ChunkProcessor {
    constructor(audio_ctx, token, key) {
        this.audio_ctx = audio_ctx;
        this.context_rate = audio_ctx.sampleRate;
        this.interval = this.context_rate * 5;

        this.input_offset = 0;
        this.input_buffer = this.create_input_buffer();
        this.output_offset = this.interval;
        this.output_buffer = null;

        this.input_queue = new Queue();
        this.output_queue = new Queue();

        if (key != null) {
            this.emitter = new Worker('emitter.js');
            this.emitter_busy = false;
            this.emitter.onmessage = (evt) => {this.emitter_callback(evt)};
            this.emitter.postMessage(key);
        } else {
            this.absorber = new Worker('absorber.js');
            this.absorber_index = 0;
            this.absorber.onmessage = (evt) => {this.absorber_callback(evt)};
            this.absorber.postMessage(token);
            this.absorber.postMessage(this.absorber_index);
        }
    }

    create_input_buffer() {
        return this.audio_ctx.createBuffer(2, this.interval, this.context_rate);
    }

    enqueue_chunk(buffer) {
        let offaud_ctx = new OfflineAudioContext(2, 44100 * 5, 44100);
        let source = offaud_ctx.createBufferSource();
        source.connect(offaud_ctx.destination);
        source.buffer = buffer;
        source.start();
        offaud_ctx.startRendering().then((outbuf) => {
            this.input_queue.enqueue(outbuf);
            this.emit_chunk();
        });
    }

    emit_chunk() {
        if (this.emitter_busy || this.input_queue.isEmpty()) {
            return;
        }
        let buffer = this.input_queue.dequeue();
        let data = new Array(buffer.numberOfChannels);
        for (let ch = 0; ch < buffer.numberOfChannels; ch++) {
            data[ch] = buffer.getChannelData(ch);
        }
        this.emitter_busy = true;
        this.emitter.postMessage(data);
    }

    emitter_callback(evt) {
        this.emitter_busy = false;
        this.emit_chunk();
    }

    absorber_callback(evt) {
        if (typeof evt.data == 'number') {
            this.absorber_index = evt.data;
        } else {
            if (evt.data != null) {
                this.test(evt.data);
            }
            this.absorber_index += 1;
        }
        this.absorber.postMessage(this.absorber_index);
    }

    test(data) {
        let offaud_ctx = new OfflineAudioContext(2, this.context_rate * 5,
            this.context_rate);
        let buffer = offaud_ctx.createBuffer(2, 44100 * 5, 44100);
        let source = offaud_ctx.createBufferSource();

        for (let ch = 0; ch < data.length; ch++) {
            buffer.copyToChannel(data[ch], ch, 0);
        }

        source.connect(offaud_ctx.destination);
        source.buffer = buffer;
        source.start();
        offaud_ctx.startRendering().then((outbuf) => {
            this.output_queue.enqueue(outbuf);
        });
    }

    dequeue_chunk() {
        if (!this.output_queue.isEmpty()) {
            $('#wait').hide();
            $('#run').show();
            return this.output_queue.dequeue();
        } else {
            $('#run').hide();
            $('#wait').show();
            return this.audio_ctx.createBuffer(2, this.interval,
                this.context_rate);
        }
    }

    write_buffer(databuf) {
        for (let off = 0; off < databuf.length; ) {
            if (this.input_offset == this.interval) {
                this.enqueue_chunk(this.input_buffer);
                this.input_buffer = this.create_input_buffer();
                this.input_offset = 0;
            }
            let len = Math.min(databuf.length - off,
                this.interval - this.input_offset);
            for (let ch = 0; ch < databuf.numberOfChannels; ch++) {
                let src = databuf.getChannelData(ch).subarray(off, off + len);
                this.input_buffer.copyToChannel(src, ch, this.input_offset);
            }
            off += len;
            this.input_offset += len;
        }
    }

    read_buffer(databuf) {
        for (let off = 0; off < databuf.length; ) {
            if (this.output_offset == this.interval) {
                this.output_buffer = this.dequeue_chunk();
                this.output_offset = 0;
            }
            let len = Math.min(databuf.length - off,
                this.interval - this.output_offset);
            for (let ch = 0; ch < databuf.numberOfChannels; ch++) {
                let src = this.output_buffer.getChannelData(ch).subarray(
                    this.output_offset, this.output_offset + len);
                databuf.copyToChannel(src, ch, off);
            }
            off += len;
            this.output_offset += len;
        }
    }
}
