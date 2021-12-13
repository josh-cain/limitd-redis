/* eslint-env node, mocha */
const LimitDB = require('../lib/db');
const assert = require('chai').assert;

const buckets = {
    ip: {
        size: 10,
    }
};

describe('LimitDBRedis', () => {
    let db;

    beforeEach((done) => {
        db = new LimitDB({uri: 'localhost', buckets, prefix: 'tests:'});
        db.once('error', done);
        db.once('ready', () => {
            db.resetAll(done);
        });
    });

    afterEach((done) => {
        db.close((err) => {
            // Can't close DB if it was never open
            if (err && err.message.indexOf('enableOfflineQueue') > 0) {
                err = undefined;
            }
            done(err);
        });
    });

    describe('TAKE', () => {

        it('correctly returns 0 remaining when a DB record already exists', (done) => {
            const params = {type: 'ip', key: '21.17.65.41'};
            db.take({...params, count: 9}, (err, result1) => {
                if (err) {
                    return done(err);
                }
                assert.equal(result1.conformant, true);
                assert.equal(result1.remaining, 1);
                db.take(params, (err, result2) => {
                    if (err) {
                        return done(err);
                    }
                    assert.equal(result2.conformant, true);
                    assert.equal(result2.remaining, 0);
                    db.take({...params, count: 5}, (err, result3) => {
                        if (err) {
                            return done(err);
                        }
                        assert.equal(result3.conformant, false);
                        assert.equal(result3.remaining, 0);
                        done();
                    })
                });
            });
        });


        it('is totally wrong when the initial TAKE would result in a negative number', (done) => {
            const params = {type: 'ip', key: '21.17.65.42'};
            db.take({...params, count: 25}, (err, result1) => {
                if (err) {
                    return done(err);
                }
                assert.equal(result1.conformant, false);
                assert.equal(result1.remaining, 0);
                done();
            });
        });
    });
});
