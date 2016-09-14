'use strict'

class queue {
    constructor() {
        this.stack = new Array();
        this.tail = 0;
    }
    get length() {
        return this.stack.length - this.tail;
    }
    push(obj) {
        this.stack.push(obj);
    }
    pop() {
        if (this.tail == this.stack.length) {
            return null;
        }
        let obj = this.stack[this.tail++];
        this.balance();
        return obj;
    }
    empty() {
        return this.length == 0;
    }
    balance() {
        if (this.stack.length <= this.tail * 2) {
            this.stack = this.stack.slice(this.tail);
            this.tail = 0;
        }
    }
}
