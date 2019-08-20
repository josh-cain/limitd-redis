const Transform = require('stream').Transform;

/**
 * Redis scan stream returns an array of keys per read, this is fine but complicates
 * building write batches of different size, this stream is a simple normalization
 * stream that takes the arrays and transform the items into individual objects'
 */

class BreakReadBatchStream extends Transform {
  constructor({ limit }) {
    super({ objectMode: true });
    this.limit = limit;
    this.counter = 0;
  }

  _transform(keys, encoding, callback) {
    if (!keys || keys.length === 0) {
      return callback();
    }

    keys.forEach((key) => {
      this.push(key)
      this.counter++;
    });

    if (this.counter <= this.limit) {
      return callback();
    }

    // Stop processing more keys
    return callback(null);
  }
};

class StatusKeyDetails extends Transform {
  constructor({ redis, type, prefixLength }) {
    super({ objectMode: true });
    this.redis = redis;
    this.type = type;
    this.prefixLength = prefixLength;
  }

  _transform(key, encoding, callback) {
    key = key.substring(this.prefixLength);
    this.redis.hmget(`${this.type}:${key}`, 'r', (err, hash) => {
      if (err) {
        return this.emit('error', err);
      }
      const remaining = parseInt(hash[0], 10);
      this.push({ key, remaining });
      return callback();
    });
  }
};

module.exports.BreakReadBatchStream = BreakReadBatchStream;
module.exports.StatusKeyDetails = StatusKeyDetails;
