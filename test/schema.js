var cassandra = require('../');


exports.User = cassandra.define('User', {
	fields: {
		email:    {type: String, index: true, unique: true, notnull: true},
		name:     {type: String, index: true},
		joinedAt: {type: Date, index: true, default: Date.now},
		approved: {type: Boolean, index: true, default: false},
		age:      {type: Number, index: true},
		bio:      String
	}
});

exports.Post = cassandra.define('Post', {
	fields: {
		date:      {type: Date, index: true},
		title:     {type: String, length: 255, index: true},
		content:   String,
		published: {type: Boolean, index: true, default: false},
		related:   [{type: 'uuid'}],
		labels:    [String]
	},
	tableName: 'posts'
});


exports.Friendships = cassandra.define('Friendships', {
	fields: {
		note: String
	},
	primaryKeys: ['userId', 'friendId']
});


exports.Settings = cassandra.define('Settings', {
	fields: {
		list: []
	}
});
