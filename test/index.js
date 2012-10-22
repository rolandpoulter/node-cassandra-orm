var assert = require('assert');

var cassandra = require('../lib'),
    Model = cassandra.Model;


console.log('\n');


var User = new Model('User', {
	fields: {
		name:     {type: String, index: true},
		bio:      {type: String},
		approved: {type: Boolean, index: true},
		joinedAt: {type: Date, index: true},
		age:      {type: Number, index: true}
	}
});


var user = new User({
	name: 'Roland',
	bio: 'Failure',
	approved: false,
	joinedAt: Date.now(),
	age: 23

}, function (err) {
	if (err) throw err;

	User.byId(user.data.id, function (err, _user) {
		if (err) throw err;

		assert(user.getId().toString() === _user.getId().toString());

		console.log('insert:', user.data);
		console.log('\n');

		console.log('select:', _user.data);
		console.log('\n');

		User.execute(User.cql.dropTable(), cassandra.close);
	});
});


cassandra.connect({
	keyspace: 'orm_test'
});


setTimeout(function () {
	assert(!cassandra.connection);
}, 1000);
