const EventEmitter = require('events').EventEmitter;
const url          = require('url');
const util         = require('util');

const disyuntor    = require('disyuntor');
const grpc         = require('grpc');
const ms           = require('ms');

const baasProto     = grpc.load('./baas.proto').baas;
const DEFAULT_PORT  = 9485;
const DEFAULT_HOST  = 'localhost';

function parseURI (uri) {
  const parsed = url.parse(uri);
  return {
    host: parsed.hostname,
    port: parseInt(parsed.port || DEFAULT_PORT, 10),
    protocol: parsed.protocol.slice(0, -1)
  };
}

function BaaSClient (options, done) {
  options = options || {};
  EventEmitter.call(this);

  if (typeof options === 'string') {
    options = parseURI(options);
  } else if (options.uri || options.url) {
    options = _.extend(options, parseURI(options.uri || options.url));
  } else {
    options.port = options.port || DEFAULT_PORT;
    options.host = options.host || DEFAULT_HOST;
  }

  this._options = options;
  this._requestCount = 0;

  if (typeof this._options.requestTimeout === 'undefined') {
    this._options.requestTimeout = ms('2s');
  }

  this._pendingRequests = 0;

  this._client = new baasProto.BaaS(`${this._options.host}:${this._options.port}`,
                                      grpc.credentials.createInsecure());
  if(done) done();
}
util.inherits(BaaSClient, EventEmitter);

BaaSClient.prototype.hash = function (password, salt, callback) {
  //Salt is keep for api-level compatibility with node-bcrypt
  //but is enforced in the backend.
  if (typeof salt === 'function') {
    callback = salt;
  }

  if (!password) {
    return setImmediate(callback, new Error('password is required'));
  }

  const request = {
    'password':  password,
    'operation': 1 // HASH
  };

  this._client.bcrypt(request, (err, response) => {
    callback(err, response && response.hash);
  });
};

BaaSClient.prototype.compare = function (password, hash, callback) {
  if (!password) {
    return setImmediate(callback, new Error('password is required'));
  }

  if (!hash) {
    return setImmediate(callback, new Error('hash is required'));
  }

  var request = {
    'password':  password,
    'hash':      hash,
    'operation': 0 // COMPARE
  };

  this._client.bcrypt(request, (err, response) => {
    callback(err, response && response.success);
  });
};

module.exports = BaaSClient;
