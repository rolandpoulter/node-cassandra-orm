var clss = require('clss');

var UUID = require('helenus').UUID,
    TimeUUID = require('helenus').TimeUUID;


module.exports = clss.create('Schema', function (def) {
	def.init = function (info, modelName) {
		this.info = info = info || this.info || {};

		this.modelName = modelName || this.modelName;

		if (!info.fields) info.fields = {};
		if (!info.tableName) info.tableName = this.modelName.toLowerCase();

		if (!info.primaryKeys) info.primaryKeys = ['id'];

		if (typeof info.primaryKeys === 'string')
			info.primaryKeys = [info.primaryKeys];

		info.primaryKeys.forEach(function (key) {
			info.fields[key] = info.fields[key] || {type: 'uuid'};
		});

		return this.cleanFields(info.fields);
	};

	def.cleanFields = function (fields) {
		if (!fields) return this;

		var that = this;

		Object.keys(fields).forEach(function (name) {
			fields[name] = cleanField(fields[name]);
		});

		return this;

		function cleanField (field) {
			if (field && !field.type) field = {type: field};

			if (Array.isArray(field.type)) {
				field = {type: Array, contains: that.cleanFields(field.type[0])};
			}

			if (typeof field.type === 'string') field.type = {name: field.type};

			else if (!field.type.name) that.cleanFields(field.type);

			return field;
		}
	}

	def.givePrimaryKeys = function () {
		// TODO: this should work like defaults work in Model

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
	  var fld = (fields || this.info.fields)[prop] || {};
		var typeName = fld.type && fld.type.name;

		return this.fixDataType(typeName);
	};

	def.fixDataType = function (typeName) {
		// TODO: this should be moved to CQLWriter
    if (!typeName)
      typeName = 'Object';
    
		switch (typeName) {
			case 'UUID': return 'uuid';

			case 'Date': return 'timestamp';

			case 'Blob':
			case 'Buffer': return 'blob';

			case 'Number': return 'double';

			case 'Boolean': return 'boolean';

			case 'JSON':
			case 'Error':
			case 'Array':
			case 'Object':
			case 'String':
			case 'RegExp':
			case 'Function': return 'text';

			case 'Null':
			case 'Undefined': return 'varchar';

			default: return typeName;
		}
	};

	def.toDatabase = function (value, name, fields) {
		// TODO: maybe this should goto CQLWriter
		if (/^token\(/.test(name)) {
		  console.log(value, name);
		  return value.toString();
		}

    if (name === 'solr_query')
      return "'" + value + "'";
    
		fields = fields || this.info.fields;

		var field = fields[name],
		    type = field && this.fixDataType(field.type.name);

    if (value === undefined || value === null) return null;
		switch (type) {
			case 'timestamp':
				if (!value.getTime) value = new Date(value);
				return value.getTime();

			case 'double': return value || 0;
			case 'int': return value || 0;
			case 'float': return value || 0;
			case 'uuid': return value.toString() || "null";
			case 'timeuuid': return value.toString() || "null";
			case 'bigint': return value.toString() || "null";

			case 'boolean': return !!value ? 'true' : 'false';

			case 'text':
				switch (field.type.name) {
					case 'RegExp':
					case 'Function': return "'" + value.toString().replace("'", "''") + "'";

					case 'Array': if (!value) value = []; break;
				}
		}
		value = value || '';
		if (typeof value === 'string')
		  return "'" + value.replace("'", "''") + "'";
		return "'" + JSON.stringify(value || '').replace("'", "''") + "'";
	};

	def.fromDatabase = function (value, name, fields) {
		// TODO: maybe this should goto Model
		if (/^token\(/.test(name)) {
		  console.log(value, name);
		  return value.toString();
		}

		fields = fields || this.info.fields;
		var field = fields[name],
		    type = field && this.fixDataType(field.type.name);

    if (value === null || value === undefined)
      return null;
		switch (type) {
			case 'timestamp': return value ? new Date(value.toString()) : null;//.replace(/GMT.*$/, 'GMT'));

			case 'uuid':
			case 'timeuuid':
				return value.toString();

			case 'boolean':
			  switch (value) {
			  case true:
			    return true;
			  case false:
			    return false;
			  default:
			    return /true|1|T/.test(value);
			  }
			case 'text':
				switch (field.type.name) {
					case 'RegExp':
					case 'Function': return eval(value); //eval(value.replace("''", "'"));

					case 'Array':
					  if (!value) value = []; break;
            try {
              //value = value.replace("''", "'");
              value = JSON.parse(value);
            } catch (err) {}
            break;
          case 'Object':
            try {
              //value = value.replace("''", "'");
              value = JSON.parse(value);
            } catch (err) {}
            break;
				}
		}

		try {
			//value = value.replace("''", "'");
			if (value.charAt(0) == '"' && value.charAt(value.length - 1) == '"')
  			value = JSON.parse(value);
		} catch (err) {}

		// TODO: getters

		return value;
	};
});


exports = module.exports;


exports.UUID = clss.call(UUID, 'UUID');
exports.TimeUUID = clss.call(TimeUUID, 'TimeUUID');
exports.Text = clss.call(String, 'Text');
exports.Blob = clss.call(Buffer, 'Blob');
exports.JSON = clss.call(JSON, 'JSON', function (def) {
	def.init = function (object) {return JSON.stringify(object);}
});

exports.Null = clss('Null', function (def) {def.init = function () {return null;}});
exports.Undefined = clss('Undefined', function (def) {def.init = function () {return;}});

