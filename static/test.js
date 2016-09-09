'use strict'

function main() {
    let e_source = $('#source')[0];
    let audio_ctx = new AudioContext();
    let source_node = audio_ctx.createMediaElementSource(e_source);

    let chunk_processor = new ChunkProcessor(audio_ctx, audio_ctx.sampleRate);

    let process_node = audio_ctx.createScriptProcessor(16384, 2, 2);
    process_node.onaudioprocess = (evt) => {
        chunk_processor.write_buffer(evt.inputBuffer);
    };

    source_node.connect(process_node);
    e_source.play();

    output(chunk_processor);
}

function output(chunk_processor) {
    let audio_ctx = new AudioContext();
    let process_node = audio_ctx.createScriptProcessor(16384, 2, 2);
    process_node.onaudioprocess = (evt) => {
        console.log('read chunk');
        chunk_processor.read_buffer(evt.outputBuffer);
    };
    process_node.connect(audio_ctx.destination);
}

class ChunkProcessor {
    constructor(audio_ctx, context_rate) {
        this.audio_ctx = audio_ctx;
        this.context_rate = context_rate;
        this.interval = context_rate * 10;

        this.input_offset = 0;
        this.input_buffer = this.audio_ctx.createBuffer(2, this.interval,
            this.context_rate);
        this.output_offset = this.interval;
        this.output_buffer = null;

        this.input_queue = new Queue();
        this.output_queue = new Queue();
        this.emitter = new Worker('emitter.js');
        this.emitter_busy = false;
        this.emitter.onmessage = (evt) => {this.emitter_callback(evt)};
    }

    enqueue_chunk(buffer) {
        let offaud_ctx = new OfflineAudioContext(2, 44100 * 10, 44100);
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
        this.test(evt.data);
    }

    test(data) {
        let offaud_ctx = new OfflineAudioContext(2, this.context_rate * 10,
            this.context_rate);
        let buffer = offaud_ctx.createBuffer(2, 44100 * 10, 44100);
        let source = offaud_ctx.createBufferSource();

        for (let ch = 0; ch < data.length; ch++) {
            buffer.copyToChannel(data[ch], ch, 0);
        }

        source.connect(offaud_ctx.destination);
        source.buffer = buffer;
        source.start();
        offaud_ctx.startRendering().then((outbuf) => {
            console.log('test');
            this.output_queue.enqueue(outbuf);
        });
    }

    dequeue_chunk() {
        if (!this.output_queue.isEmpty()) {
            return this.output_queue.dequeue();
        } else {
            return this.audio_ctx.createBuffer(2, this.interval,
                this.context_rate);
        }
    }

    write_buffer(databuf) {
        for (let off = 0; off < databuf.length; ) {
            if (this.input_offset == this.interval) {
                this.enqueue_chunk(this.input_buffer);
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
