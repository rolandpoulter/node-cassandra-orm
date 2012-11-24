require('require-stars')();

module.exports = require('spc').describe('Cassandra ORM:', function () {
	var cassandra = require('../');

	before(function () {
		should();
	});

	before(function (end) {
		this.options = {
			log: !true,
			keyspace: 'orm_test'
		};

		cassandra.connect(this.options, end);
	});

	/*
	beforeEach(function (end) {
		cassandra.connection.execute('CREATE KEYSPACE orm_test;', function (err) {
			if (err) return end(err);

			cassandra.connection.execute('USE orm_test;', end);
		});
	})

	afterEach(function (end) {
		cassandra.connection.execute('DROP KEYSPACE orm_test;', function (err) {
			if (err) return end(err);

			cassandra.close(end);
		})
	});
	*/

	add.apply(null, require('require-stars/flat')(require('./specs/**')));
});

require('spc/reporter/dot')(module.exports);
