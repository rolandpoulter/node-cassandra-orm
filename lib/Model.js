var async = require('async'),
    merge = require('mrg'),
    Emitter = require('mttr'),
    safeApply = merge.safeApply;

var orm = require('./index'),
    Schema = require('./Schema'),
    CQLWriter = require('./CQLWriter');


module.exports = require('clss')('Model', function (def) {
	var Model = this;

	Emitter.create(this);

	def.init = function (data, exists) {
		if (this.constructor === Model) return createModel.apply(this, arguments);

		if (exists !== true) this.reset().mixin(data);

		else this.data = data;

		if (typeof exists === 'function') this.save(exists);

		else this.exists = exists;

		if (!this.exists) this.constructor.emit('new instance', this);

		return this;
	};



	function createModel (name, info, proc, postPonePrepare) {
		var self = Model.clss(name, proc);

		self.schema = new Schema(info, self.name || name);
		self.cql = new CQLWriter(self.schema);

		Model.emit('new model', self);

    if (!postPonePrepare)
  		return self.prepare();
  	return self;
	}



	// Instance methods

	def.reset = function () {
		this.data = this.defaults();
		this.errors = [];

		return this;
	};

	def.defaults = function () {
		var defaults = {},
		    schema = this.constructor.schema,
		    fields = schema.info.fields,
		    id = schema.givePrimaryKeys();

		schema.info.primaryKeys.forEach(function (key, i) {
			defaults[key] = id[i];
		});

		Object.keys(fields).forEach(function (name) {
			if (fields[name]) {
				// TODO: move this default code into Schema
				if (fields[name].default !== undefined) {
					if (schema.info.primaryKeys.indexOf(name) === -1) {
						defaults[name] = fields[name].default;

						if (typeof defaults[name] === 'function' && fields[name].type.name !== 'Function') {
							defaults[name] = defaults[name]();
						}
					}

				} else if (typeof fields[name] === 'function') {
					defaults[name] = new (fields[name])();
				}
			}
		});

		return defaults;
	};

	def.mixin = function (data) {
		var keys = this.constructor.schema.info.primaryKeys;

		merge(this.data, data, function (value, name) {
			if (keys.indexOf(name) !== -1) return !value;

			// TODO: validations, and setters
		});

		return this;
	};


	def.set = function (name, value) {
		if (typeof name === 'object') return this.mixin(name);

		return this.data[name] = value;
	};

	def.get = function (name) {
		return this.data[name];
	};

	def.getId = function () {
		var id = this.constructor.schema.
			info.primaryKeys.map(this.get.bind(this));

		return id.length === 1 ? id[0] : id;
	};


	def.update = function (data, callback, options) {
		callback = callback || data;
		data = data === callback ? undefined : data;

		if (data) this.mixin(data);

		this.constructor.updateById(this.data, this.getId(), callback, options);

		return this;
	};

	def.remove =
	def.destroy = function (callback, options) {
		this.constructor.destroyById(this.getId(), callback, options);

		return this;
	};

	def.save = function (callback, options) {
		if (this.exists) return this.update(null, callback, options);

		this.constructor.insert(this.data, callback, options);

		return this;
	};

	def.sync = function (callback, consistency) {
		var that = this;

		this.constructor.byId(this.getId(), function (err, data) {
			if (!err) that.mixin(data);

			safeApply(callback, arguments);
		}, consistency);

		return this;
	};



	// Class methods

	this.prepare = function (callback) {
		var that = this;
		orm.events.on('ready', function (connection) {
			that.automigrate(function (err) {
				if (!err) that.useConnection(connection);

				safeApply(callback, arguments, that);
			}, connection);
		});
		return this;
	};


	this.useConnection = function (connection) {
		this.connection = connection;

		return this.forever('ready', connection);
	};


	this.execute = function (command, callback, that) {
		if (this.connection) return execute.call(this, this.connection);

    var self = this;
		return this.on('ready', function () {
		  execute(self.connection);
		});

		function execute(connection) {
			connection.execute(command, callback, that || this);

			return this;
		}
	};

	this.batch = function (proc, callback, that, options) {
		if (this.connection) return batch.call(this, this.connection);

    var self = this;
		return this.on('ready', function () {
		  batch(self.connection);
		});

		function batch(connection) {
			connection.batch(proc, callback, that || this, options);

			return this;
		}
	}


	this.find =
	this.all = function (filter, callback, consistency) {
		return this.execute(this.cql.selectAll(filter, consistency), function (err, rows) {
			safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)], this);
		});
	};

	this.select = function (expr, filter, callback, consistency) {
		return this.execute(this.cql.select(expr, filter, consistency), function (err, rows) {
			safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)], this);
		});
	};

	this.one = function (conds, order, offset, callback, consistency) {
		callback = callback || offset || order;
		offset = offset === callback ? undefined : offset;
		order = order === callback ? undefined : order;

		return this.execute(this.cql.selectOne(conds, order, offset, consistency), function (err, rows) {
			safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)[0]], this);
		});
	};

	this.byId = function (id, callback, consistency) {
		return this.execute(this.cql.selectById(id, consistency), function (err, rows) {
			safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)[0]], this);
		});
	};


	this.count = function (conds, limit, offset, callback, consistency) {
		callback = callback || offset || limit;
		offset = offset === callback ? undefined : offset;
		limit = limit === callback ? undefined : limit;

		return this.execute(this.cql.count(conds, limit, offset, consistency), function (err, rows) {
			safeApply(callback, [err, err ? undefined : this.fromDatabase(rows, true)[0].count], this);
		});
	};


	this.fromDatabase = function (rows, raw) {
		var schema = this.schema;

		if (!rows) return [];

		var results = [];

		rows.forEach(function (row) {
			if (!row || row.count === 0) return;

			var data = {};

			row.forEach(function (name, value, ts, ttl) {
				data[name] = schema.fromDatabase(value, name);
			});

			results.push(raw ? data : new Model(data, true));
		});

		return results;
	};


	this.insert = function (data, callback, options)  {
		return this.execute(this.cql.insert(data, options), callback);
	}


	this.update = function (data, conds, callback, options) {
		return this.execute(this.cql.update(data, conds, options), callback);
	};

	this.updateById = function (data, id, callback, options) {
		return this.execute(this.cql.updateById(data, id, options), callback);
	};


	this.destroy = function (columns, conds, callback, options) {
		return this.execute(this.cql.destroy(columns, conds, options), callback);
	};

	this.destroyAll = function (conds, callback, options) {
		return this.execute(this.cql.destroyAll(conds, options), callback);
	};

	this.destroyById = function (id, callback, options) {
		return this.execute(this.cql.destroyById(id, options), callback);
	};


	this.empty =
	this.truncate = function (callback) {
		return this.execute(this.cql.truncate(), callback);
	};


	this.alterTable = function (changes, callback) {
		return this.execute(this.cql.alterTable(changes), callback);
	};


	this.automigrate = function (callback, connection) {
		connection = connection || this.connection;

		var that = this,
		    name = this.cql.tableName(),
		    keyspace = connection.keyspace;
		return connection.execute('select * from system.schema_columnfamilies where keyspace_name=\'' + connection.keyspace.name + '\' and columnfamily_name=\'' + name + '\'', function (err, rows) {
		  if (err) {
        return keyspace.get(name, function (err, columnFamily) {
          if (err && err.name !== 'HelenusNotFoundError') {
            return safeApply(callback, [err], that);
          }
          if (columnFamily) {
            var columns = {};
            for (var i in columnFamily.columnValidators) {
              if (/^_.*/.test(i))
                continue;
              var c = columnFamily.columnValidators[i];
              var cm = null;
              columnFamily.definition.column_metadata.some(function (col) {
                cm = col;
                return col.name === i;
              });
              // type: c.type (UTF8Type for example)
              var col = {name:i, type: 'Object', index:cm.index_type || cm.index_name};
              columns[col.name] = col;
            }
            matchColumns(columns);
          } else {
            if (connection) createTable(connection);

            else that.on('ready', createTable);
          }
        });		    //cql, callback, bind, options, args
      }

		  if (rows.length > 0) {
        return connection.execute('select * from system.schema_columns where keyspace_name=\'' + connection.keyspace.name + '\' and columnfamily_name=\'' + name + '\'', function (err, rows) {
          var columns = {};
          rows.forEach(function (row) {
            var col = {};
            col.name = row.get('column_name').value.toLowerCase();
            col.index = row.get('index_name').value !== null;
            col.type = 'Object';
            columns[col.name] = col;
          });
          matchColumns(columns);
        });
      } else {
        if (connection) createTable(connection);
        else that.on('ready', createTable);
      }
    });

    function matchColumns(columns) {
      var same = true;
      var changes = {adds:[], drops:[]};
      Object.keys(that.schema.info.fields).forEach(function (name) {
        name = name.toLowerCase();
        if (!columns[name] && name !== 'id') {
          same = false;
          changes.adds.push({name:name, type:that.schema.info.fields[name].type || 'Object', index:that.schema.info.fields[name].index});
        }
      });
      Object.keys(columns).forEach(function (name) {
        if (!that.schema.info.fields[name]) {
          that.schema.info.fields[name] = columns[name];
        }
      });
        // TODO: check schema for changes and alter table
      if (same)
        createIndexes();
      else {
        async.forEach(that.cql.alterTable(changes), function (cql, next) {
            connection.execute(cql, next);
        }, createIndexes);
      }
    }

		function createTable (connection) {
			connection.execute(that.cql.createTable(), createIndexes);
		}

		function createIndexes (err) {
			if (err) {
			  return safeApply(callback, [err], that);
			}

			async.forEach(that.schema.getIndexes(), function (name, next) {
			  if (name === "id" || name === 'solr_query')
			    return next();
				connection.execute(that.cql.createIndex(name), function (err) {
					if (err && err.name !== 'HelenusInvalidRequestException') {
						// NOTE: Not sure if HelenusInvalidRequestException only throws when the index
						//       is already defined or not, but this is to avoid an error when the index already exists
						return next(err);
					}

					next();
				});
			}, callback.bind(that));
		}
	};



	this.hasOne = function (model, options) {
		// TODO:
	};

	this.hasMany = function (model, options) {
		// TODO:
	};

	this.belongsTo = function (model, options) {
		// TODO:
	};


	// TODO: validation methods
});
