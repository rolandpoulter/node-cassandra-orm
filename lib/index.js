var Connection = exports.Connection = require('./Connection'),
    Schema = exports.Schema = require('./Schema'),
    Model = exports.Model = require('./Model');

exports.CQLWriter = require('./CQLWriter');

exports.UUID = Schema.UUID;
exports.TimeUUID = Schema.TimeUUID;

exports.Text = Schema.Text;
exports.Blob = Schema.Blob;
exports.JSON = Schema.JSON;

exports.Undefined = Schema.Undefined;
exports.Null = Schema.Null;


exports.events = require('mttr').create();

exports.connection = null;

exports.connect = function (options, callback) {
	if (exports.connection) exports.close();

	return exports.connection = new Connection(options, function (err) {
		if (typeof callback === 'function') {
			callback(err, err ? undefined : exports.connection);

		} else if (err) throw err;

		if (!err) exports.events.forever('ready', exports.connection);

		Model.useConnection(exports.connection);
	});
};

exports.close = function (callback) {
	exports.connection.close(function (err) {
		if (!err) Object.keys(exports.models).forEach(function (name) {
			var model = exports.models[name];

			delete model.connection;

			model.unforever('ready').prepare();
		});

		delete Model.connection;

		if (typeof callback === 'function') callback(err);
	});

	delete exports.connection;
}


exports.models = {};

exports.define =
exports.describe = function (name, desc, proc) {
	return exports.models[name] = new Model(name, desc, proc);
};
