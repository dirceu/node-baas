const bcrypt = require('bcrypt');

const execute = module.exports = function (request) {
  const request_id = request.id;

  if (request.operation === 'COMPARE') {
    //compare
    const success = bcrypt.compareSync(request.password, request.hash);
    return {
      request_id,
      success
    };
  } else if (request.operation === 'HASH') {
    //hash
    const hash = bcrypt.hashSync(request.password, 10);
    return {
      request_id,
      hash,
      success: true
    };
  }
};

/**
 * request { id, operation, password, hash? }
 * operation { compare: 0, hash: 1}
 */
process.on('message', (request) => {
  process.send(execute(request));
});

