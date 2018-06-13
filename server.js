const cluster      = require('cluster');
const EventEmitter = require('events').EventEmitter;

const util           = require('util');
const bunyan         = require('bunyan');
const _              = require('lodash');
const Deque          = require('double-ended-queue');
const AWS            = require('aws-sdk');
const randomstring   = require('randomstring');
const grpc           = require('grpc');

const baasProto = grpc.load('./baas.proto').baas;

const defaults = {
  port:     9485,
  hostname: 'localhost',
  logLevel: 'info',
  socketTimeout: 2000,
  metrics: {
    gauge:     _.noop,
    increment: _.noop,
    histogram: _.noop,
    flush:     _.noop
  },
  logger: bunyan.createLogger({
    name:        'baas',
    level:       'error',
    serializers: bunyan.stdSerializers
  })
};

function fork_worker() {
  const worker = cluster.fork();

  worker._pendingRequests = new Map();

  worker.on('message', function (response) {
    const callback = worker._pendingRequests.get(response.request_id);
    worker._pendingRequests.delete(response.request_id);
    worker.emit('drain');
    return callback(response);
  });

  worker.sendRequest = function (message, callback) {
    worker._pendingRequests.set(message.id, callback);
    worker.send(message);
  };

  return worker;
}

/*
 * Creates an instance of BaaSServer.
 *
 * Options:
 *
 *  - `port` the port to listen to. Defaults to 9231.
 *  - `hostname` the hostname to bind to. Defaults to INADDR_ANY
 *  - `logLevel` the verbosity of the logs. Defaults to 'info'.
 *
 */
function BaaSServer (options) {
  EventEmitter.call(this);

  this._config = _.extend({}, defaults, options);
  this._logger = this._config.logger;
  this._metrics = this._config.metrics;

  this._server = new grpc.Server();
  this._server.addService(baasProto.BaaS.service, {bcrypt: this._handleMessage.bind(this)});

  cluster.setupMaster({
    exec: __dirname + '/worker.js'
  });

  var workers_number;

  if (options.workers) {
    workers_number = options.workers;
  } else if (!isNaN(process.env.WORKERS)) {
    workers_number = parseInt(process.env.WORKERS, 10);
  } else {
    workers_number = Math.max(require('os').cpus().length - 1, 1);
  }

  this._queue = new Deque();
  this._workers = _.range(workers_number).map(fork_worker);

  this._workers.forEach(worker => {
    worker.on('drain', () => {
      const pending = this._queue.shift();
      if (!pending) {
        this._workers.push(worker);
      } else {
        worker.sendRequest(pending.request, pending.done(worker.id, true));
      }
    });
  });

  if (process.env.REPORT_QUEUE_LENGTH) {
    this.cloudWatch = new AWS.CloudWatch();
    this._intervalQueued = 0;
    setInterval(this._reportQueueLength.bind(this), 1000);
  }
}

util.inherits(BaaSServer, EventEmitter);

BaaSServer.prototype._reportQueueLength = function () {
  this._metrics.gauge('requests.queued', this._queue.length);

  const dimensions = [];

  if (process.env.STACK_NAME) {
    dimensions.push({
      Name: 'StackName',
      Value: process.env.STACK_NAME
    });
  }

  const intervalQueued = this._intervalQueued;
  this._intervalQueued = 0;

  this.cloudWatch.putMetricData({
    MetricData: [
      {
        MetricName: 'QueuedRequests',
        Unit:       'Count',
        Timestamp:  new Date(),
        Value:      intervalQueued,
        Dimensions: dimensions
      }
    ],
    Namespace: 'Auth0/BAAS'
  }, (err) => {
    if (err) {
      this._logger.error({ err }, err.message);
    }
  });
};

BaaSServer.prototype._handleMessage = function (call, callback) {
  this._metrics.increment('connection.incoming');
  const request = call.request;
  const operation = request.operation === 0 ? 'compare' : 'hash';
  const start = new Date();

  if(!request.id) {
    request.id = randomstring.generate(5);
  }

  this._logger.info({
    request:   request.id,
    operation: operation,
    log_type:  'request',
    queued:    this._workers.length === 0
  }, `incoming ${operation}`);

  if (!this._workers.length){
    this._intervalQueued++;
    // no available workers, queue and wait
    this._queue.push({request, callback});
    return;
  }

  // all workers are the same, pop is faster than shift: http://stackoverflow.com/questions/6501160/why-is-pop-faster-than-shift
  const worker = this._workers.pop();

  var self = this;
  worker.sendRequest(request, function(message) {
    self._logger.info({
      request:   request.id,
      took:      new Date() - start,
      worker:    worker.id,
      operation: operation,
      log_type:  'response'
    }, `${operation} completed`);

    self._metrics.histogram(`requests.processed.${operation}.time`, (new Date() - start));
    self._metrics.increment(`requests.processed.${operation}`);
    callback(null, message);
  });
};

BaaSServer.prototype.start = function (done) {
  const log = this._logger;
  const address = `${this._config.hostname}:${this._config.port}`;
  this._server.bind(address, grpc.ServerCredentials.createInsecure());
  this._server.start();

  log.info(address, 'server started');
  this.emit('started', address);
  if (done) { return done(null, address); }

  return this;
};

BaaSServer.prototype.stop = function (done) {
  const log = this._logger;
  const address = `${this._config.host}:${this._config.port}`;

  const timeout = setTimeout(() => {
    this._server.forceShutdown();
  }, 500);

  this._server.tryShutdown(() => {
    clearTimeout(timeout);
    log.debug(address, 'server closed');
    this.emit('close');
    if (done) { done(); }
  });
};


module.exports = BaaSServer;
