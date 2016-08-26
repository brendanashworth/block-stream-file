var inherits = require('inherits');
var Transform = require('readable-stream').Transform;
var defined = require('defined');
var fs = require('fs');
var temp = require('tmp');
var assert = require('assert');

module.exports = Block;
inherits(Block, Transform);

function Block (size, opts) {
    if (!(this instanceof Block)) return new Block(size, opts);
    Transform.call(this);
    if (!opts) opts = {};
    if (typeof size === 'object') {
        opts = size;
        size = opts.size;
    }
    this.size = size || 512;
    
    if (opts.nopad) this._zeroPadding = false;
    else this._zeroPadding = defined(opts.zeroPadding, true);
    
    // Create a temporary file for this stream to buffer in.
    this._buffer = temp.fileSync();

    // _bufferedBytes is the length of the buffer file, while _bufferPosition
    // is the position at which we have not yet flushed the buffer downstream.
    this._bufferedBytes = 0;
    this._bufferPosition = 0;
}

Block.prototype._transform = function (buf, enc, next) {
    this._bufferedBytes += buf.length;

    // Write this new data to the temporary file.
    var stream = this;
    fs.write(this._buffer.fd, buf, 0, buf.length, this._bufferBytes, function(err) {
        if (err) {
            return next(err);
        }

        // Try to flush data downstream.
        tryFlush(stream, next);
    });    
};

// Try to flush from the underlying file downstream — unrelated to #_flush().
// This function will recur in the event loop until we cannot flush anymore.
function tryFlush(stream, next) {
    // Stop recursion if we don't want to flush anymore.
    if (stream._bufferedBytes < stream.size) {
        return next();
    }

    var buf = new Buffer(stream.size);
    buf.fill(0);

    fs.read(stream._buffer.fd, buf, 0, buf.length, stream._bufferPosition, function(err, bytesRead, buffer) {
        if (err) {
            return next(err);
        }

        // This is a must-have. It should always be this way, unless the temporary file was
        // modified under our noses.
        assert(bytesRead == stream.size, 'block-stream-file: bytesRead must equal buffering size');

        stream._bufferedBytes -= bytesRead;
        stream._bufferPosition += bytesRead;

        stream.push(buffer);

        // We flushed one chunk, but may have more available. Try again?
        tryFlush(stream, next);
    });
}

Block.prototype._flush = function (callback) {
    // Nothing to do.
    if (!this._bufferedBytes) return callback();

    // If we want to pad with zeroes, instantiate the Buffer to be larger.
    var zeroes = new Buffer(this._zeroPadding ? this.size : this._bufferedBytes);
    zeroes.fill(0);

    // Read into the buffer filled with zeroes, so that we overwrite the zeroes with
    // data that we have but leave the rest zeroed out.
    // If we pass null, we'll only get whats left, without the zeroes.
    var stream = this;
    fs.read(this._buffer.fd, zeroes, 0, this._bufferedBytes, this._bufferPosition, function(err, bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        // Check to be sure we read the right amount.
        assert(bytesRead == stream._bufferedBytes, 'block-stream-file: bytesRead must equal left bytes');

        stream.push(buffer);
        stream.push(null);

        // Remove the temporary file.
        stream._buffer.removeCallback();

        callback();
    });
};
