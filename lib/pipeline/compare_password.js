const bcrypt = require('bcrypt');

module.exports = function (options, request, log) {
  const metrics = options.metrics;

  return through2.obj(function (message, enc, callback) {

    if (!(message instanceof RequestMessage)) {
      return callback(null, message);
    }

    if (message.operation !== RequestMessage.Operation.COMPARE) {
      return callback(null, message);
    }

    metrics.increment('requests.incoming.compare');

    log.debug({
      request: message.id,
      connection: socket._connection_id
    }, 'comparing password');

    const start = new Date();

    bcrypt.compare(message.password, message.hash, function (err, success) {
      if (err) { return callback(err); }

      log.info({
        request:    message.id,
        connection: socket._connection_id,
        took:       new Date() - start,
        result:     success
      }, 'password compared');
      const response = new Response({
        request_id: message.id,
        success:    success,
      });

      metrics.histogram('requests.processed.compare.time', (new Date() - start));
      metrics.increment('requests.processed.compare');

      callback(null, response);
    });
  });
};
