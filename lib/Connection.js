var helenus = require('helenus'),
    ConnectionPool = helenus.ConnectionPool,
    Connection = helenus.Connection;

var cassandra = require('./index'),
    CQLWriter = require('./CQLWriter');


module.exports = require('clss').create('Connection', function (def) {
  def.init = function (options, callback) {
    var that = this;

    this.options = options = options || {};

    this.client = options.hosts ? new ConnectionPool(options) : new Connection(options);

    this.client.on('error', function (error) {
      cassandra.events.emit('error', error);
    });

    this.client.connect(function (err, keyspace) {
      if (err) {
        return end(err, keyspace);
      }
      that.version = that.client.version || (that.client.clients && that.client.clients[0] && that.client.clients[0].version);
      that.isCql3 = that.options.cqlVersion === '3.0.0' && that.version[0] === '19' && that.version[1] > '33';
      if (options.keyspace && err && err.name === 'HelenusNotFoundException') {
        return that.execute(new CQLWriter().createKeyspace(options.keyspace, options.strategy, options.keyspaceOptions),
          function (err) {
            if (err) {
              return that.execute(new CQLWriter().createKeyspace3(options.keyspace, options.strategy, options.keyspaceOptions),
                function (err) {
                  if (err) return end(err);
                  that.client.use(options.keyspace, end);
                });
            }
            that.client.use(options.keyspace, end);
        });
//        return that.client.createKeyspace(
//          options.keyspace,
//          options.keyspaceOptions,
//
//          function (err) {
//            if (err) return end(err);
//
//            that.client.use(options.keyspace, end);
//          }
//        );
      }
      end(err, keyspace);
    });

    return this;

    function end (err, keyspace) {
      that.keyspace = keyspace;

      if (typeof callback === 'function') callback.call(that, err, keyspace);
    }
  };

  def.close = function (callback) {
    this.client.on('close', callback.bind(this));
    this.client.close();

    return this;
  };

  def.execute = function (cql, callback, bind, options, args) {
    var client = this.client,
        that = this;

    if (that.options.logger) {
      var prefix = "";
      switch ((options && options.consistencylevel) || this.options.consistencylevel) {
      case cassandra.ConsistencyLevel.ALL:
        prefix = "(ALL) ";
        break;
      case cassandra.ConsistencyLevel.ONE:
        prefix = "(ONE) ";
        break;
      case cassandra.ConsistencyLevel.LOCAL_QUORUM:
        prefix = "(LOCAL_QUORUM) ";
        break;
      case cassandra.ConsistencyLevel.EACH_QUORUM:
        prefix = "(EACH_QUORUM) ";
        break;
      case cassandra.ConsistencyLevel.ANY:
        prefix = "(ANY) ";
        break;
      case cassandra.ConsistencyLevel.TWO:
        prefix = "(TWO) ";
        break;
      case cassandra.ConsistencyLevel.THREE:
        prefix = "(THREE) ";
        break;
      }
      that.options.logger.log(prefix + cql);
    }
          
    bind = bind || this;

    if (this.statements) this.statements.push(cql);

    else client.cql(cql, args || null, options || {}, end);

    return this;

    function end (err, rows) {
      if (that.options.log) {
        //console.log('\n');
        console.log(cql);
        //console.log(err);
        //console.log(rows, rows && rows.map);
      }

      if (typeof callback === 'function') return callback.call(bind, err, rows);
    }
  };

  def.batch = function (proc, callback, that, batchOptions, execOptions, args) {
    if (typeof proc !== 'function') return this;

    var statements = this.statements = [];

    proc.call(that);

    delete this.statements;

    return this.execute(
      new CQLWriter().batch(statements, batchOptions), callback, that, execOptions, args);
  };
});
