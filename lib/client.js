const _ = require('lodash');
const retry = require('retry');
const cbControl = require('cb');
const utils = require('./utils');
const LimitDBRedis = require('./db');
const disyuntor = require('disyuntor');
const EventEmitter = require('events').EventEmitter;

const ValidationError = utils.LimitdRedisValidationError;

const circuitBreakerDefaults = {
  timeout: '0.25s',
  maxFailures: 50,
  cooldown: '1s',
  maxCooldown: '3s',
  name: 'limitr',
  trigger: (err) => {
    return !(err instanceof ValidationError);
  }
};

const retryDefaults = {
  retries: 3,
  minTimeout: 10,
  maxTimeout: 30
};

class LimitdRedis extends EventEmitter {
  constructor(params) {
    super();

    this.db = new LimitDBRedis(_.pick(params, ['uri', 'nodes', 'buckets', 'prefix', 'slotsRefreshTimeout', 'password', 'tls', 'dnsLookup', 'globalTTL']));

    this.db.on('error', (err) => {
      this.emit('error', err);
    });

    this.db.on('ready', () => {
      this.emit('ready');
    });

    this.breakerOpts = _.merge(circuitBreakerDefaults, params.circuitbreaker);
    this.statusBreakerOpts = _.merge(circuitBreakerDefaults, params.statusCircuitbreaker);
    this.retryOpts = _.merge(retryDefaults, params.retry);
    // ioredis is implementing this configuration, when it's stable we can switch to it
    this.commandTimeout = params.commandTimeout || 75;

    this.dispatch = disyuntor(this.dispatch.bind(this), this.breakerOpts);
    this.statusDispatch = disyuntor(this.statusDispatch.bind(this), this.statusBreakerOpts);
  }

  static buildParams(type, key, opts, cb) {
    const params = { type, key };
    const optsType = typeof opts;

    // handle lack of opts and/or cb
    if (cb == null) {
      if (optsType === 'function') {
        cb = opts;
        opts = undefined;
      } else {
        cb = _.noop;
      }
    }

    if (optsType === 'number' || opts === 'all') {
      params.count = opts;
    }

    if (optsType === 'object') {
      _.assign(params, opts);
    }

    return [params, cb];
  }

  handler(method, type, key, opts, cb) {
    let [params, callback] = LimitdRedis.buildParams(type, key, opts, cb);
    const dispatch = method === 'status' ? this.statusDispatch : this.dispatch;
    dispatch(method, params, callback);
  }

  dispatch(method, params, cb) {
    const operation = retry.operation(this.retryOpts);
    operation.attempt(() => {
      this.db[method](params, cbControl((err, results) => {
        if (err instanceof ValidationError) {
          return cb(err);
        }

        if (operation.retry(err)) {
          return;
        }

        return cb(err ? operation.mainError() : null, results);
      }).timeout(this.commandTimeout));
    });
  }

  statusDispatch(method, params, cb) {
    // method is only used to maintain the contract
    // status won't have retries and has its own circuit breaker
    // it has a higher timeout value (250 by default instead of 75)
    this.db.status(params, cb);
  }

  take(type, key, opts, cb) {
    this.handler('take', type, key, opts, cb);
  }

  wait(type, key, opts, cb) {
    this.handler('wait', type, key, opts, cb);
  }

  get(type, key, opts, cb) {
    this.handler('get', type, key, opts, cb);
  }

  put(type, key, opts, cb) {
    this.handler('put', type, key, opts, cb);
  }

  status(type, key, opts, cb) {
    this.handler('status', type, key, opts, cb);
  }

  reset(type, key, opts, cb) {
    this.put(type, key, opts, cb);
  }

  resetAll(cb) {
    this.db.resetAll(cb);
  }

  close(callback) {
    this.db.close((err) => {
      this.db.removeAllListeners();
      callback(err);
    });
  }
}

module.exports = LimitdRedis;
module.exports.ValidationError = ValidationError;
