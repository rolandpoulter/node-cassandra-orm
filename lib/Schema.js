var UUID = require('helenus').UUID,
    TimeUUID = require('helenus').TimeUUID;


module.exports = require('clss').create('Schema', function (def) {
	def.init = function (info, modelName) {
		this.info = info = info || this.info || {};

		this.modelName = modelName || this.modelName;

		if (!info.fields) info.fields = {};
		if (!info.tableName) info.tableName = modelName;

		if (!info.primaryKeys) info.primaryKeys = ['id'];

		if (typeof info.primaryKeys === 'string')
			info.primaryKeys = [info.primaryKeys];

		info.primaryKeys.forEach(function (key) {
			info.fields[key] = info.fields[key] || {type: 'uuid'};
		});

		return this.cleanFields();
	};

	def.cleanFields = function (fields) {
		fields = fields || this.info.fields;

		Object.keys(fields).forEach(function (name) {
			var field = fields[name];

			if (field && !field.type) field = {type: field};

			if (field.type && !field.type.name) field.type = {name: field.type};

			if (!field.type.name) delete fields[name];

			fields[name] = field;
		});

		return this;
	}

	def.givePrimaryKeys = function () {
		if (typeof this.info.givePrimaryKeys === 'function') {
			return this.info.givePrimaryKeys.apply(this, arguments);
		}

		return [new UUID()];
	};

	def.getIndexes = function () {
		var fields = this.info.fields,
		    indexes = [];

		Object.keys(fields).forEach(function (name) {
			if (fields[name] && fields[name].index) indexes.push(name);
		});

		return indexes;
	};

	def.dataType = function (prop, fields) {
		var typeName = (fields || this.info.fields)[prop].type.name;

		return this.fixDataType(typeName);
	};

	def.fixDataType = function (typeName) {
		switch (typeName) {
			case 'UUID': return 'uuid';

			case 'Date': return 'timestamp';

			case 'Blob':
			case 'Buffer': return 'blob';

			case 'Number': return 'double';

			case 'Boolean': return 'boolean';

			case 'Null':
			case 'Text':
			case 'JSON':
			case 'Error':
			case 'Array':
			case 'Object':
			case 'String':
			case 'RegExp':
			case 'Function':
			case 'Undefined': return 'text';

			default: return typeName;
		}
	};

	def.toDatabase = function (value, name, fields) {
		fields = fields || this.info.fields;

		var field = fields[name],
		    type = this.fixDataType(field.type.name);

		switch (type) {
			case 'timestamp':
				if (!value) return 0;
				if (!value.getTime) value = new Date(value);
				return value.getUTCTime ? value.getUTCTime() : value.getTime();

			case 'double': return value || 0;
			case 'uuid': return value && value.toString() || "''";

			case 'text':
				switch (field.type.name) {
					case 'RegExp':
					case 'Function': return "'" + value.toString() + "'";
				}

				return "'" + JSON.stringify(value || '') + "'";
		}

		return "'" + (value || '') + "'";
	};

	def.fromDatabase = function (value, name, fields) {
		fields = fields || this.info.fields;

		var field = fields[name],
		    type = this.fixDataType(field.type.name);

		if (value !== undefined && value !== null) {
			switch (type) {
				case 'timestamp': return new Date(value.toString().replace(/GMT.*$/, 'GMT'));

				case 'uuid':
					return value;

				case 'boolean':
				case 'text':
					switch (field.type.name) {
						case 'RegExp':
						case 'Function': return eval(value);
					}

					return JSON.parse(value);
			}
		}

		return value;
	};
});

module.exports.UUID = UUID;
module.exports.TimeUUID = TimeUUID;

