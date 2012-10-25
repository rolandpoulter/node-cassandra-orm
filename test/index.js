module.exports = require('spc').describe('Cassandra ORM:', function () {
	var cassandra = require('../');

	before(function (end) {
		should();

		this.options = {
			keyspace: 'orm_test',
			log: !true
		};

		cassandra.connect(this.options, end);
	});

	after(function (end) {
		cassandra.connection.execute('DROP KEYSPACE orm_test;', function (err) {
			if (err) return end(err);

			cassandra.close(end);
		})
	});

	add(
		require('./crud'),
		require('./datatypes'),
		require('./relationships'),
		require('./validations'),
		require('./connection'),
		require('./other')
	);
});

require('spc/reporter/dot')(module.exports);
