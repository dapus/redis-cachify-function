var async = require('async');
var RedisLock = require('redis-lock');

var LOCK_TTL = 60 * 1000;

/**
 * Return a new function which gets cached data from redis or executes the input function
 *
 * @param {Function} cachedFunc - Function whose return data will be cached in redis
 * @param {Object} options
 * @param {String} options.key - Store cache under this key in redis
 * @param {Object} options.redis - A redis client
 * @param {Number} options.ttl - Cache timeout
 * @param [Number] options.lockTTL - Max time to keep the lock
 * @param [Boolean] options.lock - whether to lock while running function
 * @returns {Function}
 */
module.exports = function(cachedFunc, options) {
  var cacheKey = options.key;
  var lockKey = cacheKey + ':lock';
  var redis = options.redis;
  var lock = RedisLock(redis, 25);
  var ttl = options.ttl;
  var lockTTL = options.lockTTL || LOCK_TTL;
  var getLock = options.lock === undefined ? true : options.lock;

  var getCache = function(callback) {
    async.waterfall([
      function(cb) {
        redis.get(cacheKey, function(err, cache) {
          if(cache) {
            cache = JSON.parse(cache);
          }

          cb(err, cache);
        });
      },
      function(cache, cb) {
        if(cache || !getLock) {
          return cb(null, cache);
        }

        lock(lockKey, lockTTL, function(release) {
          cb(null, cache, release);
        });
      },
    ], function(err, cache, release) {
      if(err && release) {
        release();
      }

      callback(err, cache, release);
    });
  };

  var setCache = function(data, callback) {
    redis.set(cacheKey, JSON.stringify(data), 'EX', ttl, callback);
  };

  // return a new function which calls that function and wraps its callback
  return function() {
    var args = Array.prototype.slice.call(arguments);
    var callback = args[args.length-1];

    getCache(function(err, cache, release) {
      if(err || cache) {
        return callback(err, cache);
      }

      var finish = function(err, data) {
        if(release) {
          release();
        }

        callback(err, data);
      };

      args[args.length-1] = function(err, data) {
        if(err) {
          return finish(err);
        }

        setCache(data, function(err) {
          finish(err, data);
        });
      };

      cachedFunc.apply(null, args);
    });
  };
};

/**
 * Invalidate cache (delete redis key)
 *
 * @param {String} key - Store cache under this key in redis
 * @param {Object} redis - A redis client
 * @param [Function] callback - called when done
 */
module.exports.invalidate = function(key, redis, callback) {
  callback = callback || function() {};

  redis.del(key, callback);
};

