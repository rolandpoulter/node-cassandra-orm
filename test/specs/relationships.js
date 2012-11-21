module.exports = require('spc').describe('Relationships:', function () {
	var schema = require('../schema');

	with (schema) {
		User.hasMany(Post, {as: 'posts', via: 'userId'});
		User.hasMany(Friendships, {via: 'userId'});

		User.hasOne(Settings, {as: 'settings', via: 'userId'});


		Post.belongsTo(User, {as: 'author', via: 'userId'});

		Friendships.belongsTo(User);
		Friendships.belongsTo(User, {as: 'friend'});

		Settings.belongsTo(User, {required: true});
	}

	
});
