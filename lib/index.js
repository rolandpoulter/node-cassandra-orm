var Connection = exports.Connection = require('./Connection'),
    Model = exports.Model = require('./Model');

exports.Schema = require('./Schema');
exports.CQLWriter = require('./CQLWriter');

exports.events = require('mttr').create();

console.log(exports.events);

exports.connection = null;

exports.connect = function (options, callback) {
	if (exports.connection) exports.close();

	return exports.connection = new Connection(options, function (err) {
		if (typeof callback === 'function') {
			callback(err, err ? undefined : exports.connection);

		} else if (err) throw err;

		if (!err) exports.events.forever('ready', exports.connection);
	});
};

exports.close = function (callback) {
	exports.connection.close(function (err) {
		if (!err) Object.keys(exports.models).forEach(function (name) {
			exports.models[name].unforever('ready').prepare();
		});

		if (typeof callback === 'function') callback(err);
	});

	delete exports.connection;
}

exports.models = {};

exports.describe = function (name, desc, proc) {
	return exports.models[name] = new Model(name, desc, proc);
};
