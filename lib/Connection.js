var helenus = require('helenus'),
    ConnectionPool = helenus.ConnectionPool,
    Connection = helenus.Connection;

var events = require('./index').events,
    CQLWriter = require('./CQLWriter');


module.exports = require('clss').create('Connection', function (def) {
	def.init = function (options, callback) {
		var that = this;

		this.options = options = options || {};

		this.client = options.hosts ? new ConnectionPool(options) : new Connection(options);

		this.client.on('error', function (error) {
			events.emit('error', error);
		});

		this.client.connect(function (err, keyspace) {
			if (options.keyspace && err && err.name === 'HelenusNotFoundException') {
				return that.client.createKeyspace(
					options.keyspace,
					options.keyspaceOptions,

					function (err) {
						if (err) return end(err);

						that.client.use(options.keyspace, end);
					}
				);
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
		this.client.close(callback.bind(this));

		return this;
	};

	def.execute = function (cql, callback, that, options, args) {
		var client = this.client;

		that = that || this;

		if (this.statements) this.statements.push(arguments);

		else client.cql(cql, args || [], options || {}, end);

		return this;

		function end (err, rows) {
			//console.log('\n');
			console.log(cql);
			//console.log(err);
			//console.log(rows, rows && rows.map);
			console.log('\n');

			if (typeof callback === 'function') return callback.call(that, err, rows);
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
