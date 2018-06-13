'use strict';

const BaaSServer = require('..').Server;
const BaaSPool = require('../client');
const freeport = require('freeport');
const assert = require('chai').assert;
const _ = require('lodash');
const async = require('async');

describe('when the client is not removed from the pool', function () {
  let client;
  let server;

  before(function (done) {
    freeport(function (err, port) {
      if (err) { return done(err); }
      server = new BaaSServer({ port, logLevel: 'error' });

      server.start(function (err, address) {
        if (err) return done(err);
        client = new BaaSPool(_.extend(address, {
          pool: {
            maxConnections: 1,
            maxRequestsPerConnection: 2
          }
        }));
        done();
      });
    });
  });


  after(function (done) {
    client.disconnect();
    server.stop(done);
  });


  it('should not timeout', function(done) {
    let client_closed  = 0;

    client.on('client_closed', (c) => {
      client_closed++;
      assert.equal(c._requestCount, 2, 'it should have made two requests with the last client');
    });

    async.series([
      cb => client.hash('pass', cb),
      cb => client.hash('pass', cb),
      cb => client.hash('pass', cb),
      cb => client.hash('pass', cb),
    ], (err) => {
      if (err) { return done(err); }
      assert.equal(client._clients.length, 1, 'it should have only one client');
      assert.equal(client_closed, 1, 'it should have closed only one client');
      assert.equal(client._clients[0]._requestCount, 2, 'it should have made two requests with the last client');
      client.removeAllListeners('client_closed');
      done();
    });
  });

});
