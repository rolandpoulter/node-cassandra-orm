var async = require('async'),
    schema = require('./schema'),

    users,
    posts,
    friendships,
    settings;


with (schema) {
	users = exports.users = [
		new User({email: 'email0', name: 'name0', age: 10, bio: 'bio0'}),
		new User({email: 'email1', name: 'name1', age: 11, bio: 'bio1'}),
		new User({email: 'email2', name: 'name2', age: 12, bio: 'bio2'}),
		new User({email: 'email3', name: 'name3', age: 13, bio: 'bio3'}),
		new User({email: 'email4', name: 'name4', age: 14, bio: 'bio4'})
	];

	posts = exports.posts = [
		new Post({userId: users[0].getId(), title: 'tile0', content: 'content0'}),
		new Post({userId: users[0].getId(), title: 'tile1', content: 'content1'}),
		new Post({userId: users[2].getId(), title: 'tile2', content: 'content2'}),
		new Post({userId: users[3].getId(), title: 'tile3', content: 'content3'}),
		new Post({userId: users[4].getId(), title: 'tile4', content: 'content4'})
	];
	
	friendships = exports.friendships = [
		new Friendships({userId: users[0].getId(), friendId: users[1].getId()}),
		new Friendships({userId: users[1].getId(), friendId: users[0].getId()}),
		new Friendships({userId: users[0].getId(), friendId: users[0].getId()}),
		new Friendships({userId: users[2].getId(), friendId: users[3].getId()}),
		new Friendships({userId: users[3].getId(), friendId: users[2].getId()})
	];

	settings = exports.settings = [
		new Settings({userId: users[0].getId(), list: [0]}),
		new Settings({userId: users[1].getId(), list: [0,1]}),
		new Settings({userId: users[2].getId(), list: [0,1,2]}),
		new Settings({userId: users[3].getId(), list: [0,1,2,3]}),
		new Settings({userId: users[4].getId(), list: [0,1,2,3,4]})
	];
}


exports.save = function (name, callback) {
	async.forEach(exports[name], function (inst, next) {
		inst.save(next);
	}, callback);
};

exports.saveAll = function (callback) {
	var all = [];

	all.concat(users);
	all.concat(posts);
	all.concat(friendships);
	all.concat(settings);

	async.forEach(all, function (inst, next) {
		inst.save(next);
	}, callback);
};
