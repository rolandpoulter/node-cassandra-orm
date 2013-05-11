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


  this.execute = function (command, callback, that, options) {
    if (this.connection) return execute.call(this, this.connection);

    var self = this;
    return orm.events.on('ready', function (connection) {
      execute(connection);
    });

    function execute(connection) {
      connection.execute(command, callback, self || this, options);

      return this;
    }
  };

  this.batch = function (proc, callback, that, options) {
    if (this.connection) return batch.call(this, this.connection);

    var self = this;
    return this.on('ready', function (connection) {
      batch(connection);
    });

    function batch(connection) {
      connection.batch(proc, callback, self || this, options);

      return this;
    }
  }


  this.find =
  this.all = function (filter, callback, consistency) {
    var options;
    if (this.connection && this.connection.isCql3 && consistency) {
      options = {consistencylevel:consistency};
      consistency = undefined;
    }
    return this.execute(this.cql.selectAll(filter, consistency), function (err, rows) {
      safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)], this);
    }, undefined, options);
  };

  this.select = function (expr, filter, callback, consistency) {
    var options;
    if (this.connection && this.connection.isCql3 && consistency) {
      options = {consistencylevel:consistency};
      consistency = undefined;
    }
    return this.execute(this.cql.select(expr, filter, consistency), function (err, rows) {
      safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)], this);
    }, undefined, options);
  };

  this.one = function (conds, order, offset, callback, consistency) {
    callback = callback || offset || order;
    offset = offset === callback ? undefined : offset;
    order = order === callback ? undefined : order;
    var options;
    if (this.connection && this.connection.isCql3 && consistency) {
      options = {consistencylevel:consistency};
      consistency = undefined;
    }

    return this.execute(this.cql.selectOne(conds, order, offset, consistency), function (err, rows) {
      safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)[0]], this);
    }, undefined, options);
  };

  this.byId = function (id, callback, consistency) {
    var options;
    if (this.connection && this.connection.isCql3 && consistency) {
      options = {consistencylevel:consistency};
      consistency = undefined;
    }
    return this.execute(this.cql.selectById(id, consistency), function (err, rows) {
      safeApply(callback, [err, err ? undefined : this.fromDatabase(rows)[0]], this);
    }, undefined, options);
  };


  this.count = function (conds, limit, offset, callback, consistency) {
    callback = callback || offset || limit;
    offset = offset === callback ? undefined : offset;
    limit = limit === callback ? undefined : limit;

    var options;
    if (this.connection && this.connection.isCql3 && consistency) {
      options = {consistencylevel:consistency};
      consistency = undefined;
    }
    return this.execute(this.cql.count(conds, limit, offset, consistency), function (err, rows) {
      safeApply(callback, [err, err ? undefined : this.fromDatabase(rows, true)[0].count], this);
    }, undefined, options);
  };


  this.fromDatabase = function (rows, raw) {
    var schema = this.schema;

    if (!rows) return [];

    var results = [];

    rows.forEach(function (row) {
      if (!row || row.count === 0) return;

      var data = {};

      row.forEach(function (name, value, ts, ttl) {
        if (/^_.*/.test(name)) return;
        data[name] = schema.fromDatabase(value, name);
      });

      results.push(raw ? data : new Model(data, true));
    });

    return results;
  };


  this.insert = function (data, callback, options)  {
    return this.execute(this.cql.insert(data, options), callback, undefined, options);
  }


  this.update = function (data, conds, callback, options) {
    return this.execute(this.cql.update(data, conds, options), callback, undefined, options);
  };

  this.updateById = function (data, id, callback, options) {
    return this.execute(this.cql.updateById(data, id, options), callback, undefined, options);
  };


  this.destroy = function (columns, conds, callback, options) {
    return this.execute(this.cql.destroy(columns, conds, options), callback, undefined, options);
  };

  this.destroyAll = function (conds, callback, options) {
    return this.execute(this.cql.destroyAll(conds, options), callback, undefined, options);
  };

  this.destroyById = function (id, callback, options) {
    return this.execute(this.cql.destroyById(id, options), callback, undefined, options);
  };


  this.empty =
  this.truncate = function (callback, options) {
    return this.execute(this.cql.truncate(), callback, undefined, options);
  };


  this.alterTable = function (changes, callback, options) {
    return this.execute(this.cql.alterTable(changes), callback, undefined, options);
  };


  this.automigrate = function (callback, connection) {
    connection = connection || this.connection;
    
    if (!connection) {
      var self = this;
      return process.nextTick(function () {
        self.automigrate (callback, connection);
      });
    }
    if (this.migrating) {
      var self = this;
      return process.nextTick(function () {
        self.automigrate (callback, connection);
      });
    }
    this.migrating = true;
    
    var that = this,
        name = this.cql.tableName(),
        keyspace = connection.options.keyspace;
    return connection.execute('select * from system.schema_columnfamilies where keyspace_name=\'' + keyspace + '\' and columnfamily_name=\'' + name + '\'', function (err, rows) {
      if (err) {
        return connection.keyspace.get(name, function (err, columnFamily) {
          delete connection.keyspace.columnFamilies[name]; // Don't cache me
          if (err && err.name !== 'HelenusNotFoundError') {
            that.migrating = false;
            return safeApply(callback, [err], that);
          }
          if (columnFamily) {
            var columns = {};
            columns.id = {type:'uuid', name:'id'};
            for (var i in columnFamily.columnValidators) {
              if (/^_.*/.test(i) || i === 'id')
                continue;
              var c = columnFamily.columnValidators[i];
              var cm = null;
              columnFamily.definition.column_metadata.some(function (col) {
                cm = col;
                if (col.name === i) {
                  return true;
                }
                return false;
              });
              i = i.toLowerCase();
              var type;
              switch (cm.validation_class) {
              case 'org.apache.cassandra.db.marshal.BytesType':
                type = 'blob';
                break;
              case 'org.apache.cassandra.db.marshal.BooleanType':
                type = 'boolean';
                break;
              case 'org.apache.cassandra.db.marshal.UTF8Type':
                type = ['text', 'varchar'];
                break;
              case 'org.apache.cassandra.db.marshal.FloatType':
                type = 'float';
                break;
              case 'org.apache.cassandra.db.marshal.DoubleType':
                type = 'double';
                break;
              case 'org.apache.cassandra.db.marshal.DecimalType':
                type = 'decimal';
                break;
              case 'org.apache.cassandra.db.marshal.DateType':
                type = 'timestamp';
                break;
              case 'org.apache.cassandra.db.marshal.UUIDType':
                type = 'uuid';
                break;
              case 'org.apache.cassandra.db.marshal.LongType':
                type = 'bigint';
                break;
              case 'org.apache.cassandra.db.marshal.IntegerType':
                type = 'varint';
                break;
              case 'org.apache.cassandra.db.marshal.Int32Type':
                type = 'int';
                break;
              default:
                console.log('Unknown type: ', cm.validation_class);
                type = 'Object';
                break;
              }
              // type: c.type (UTF8Type for example)
              var class_name = (cm.index_options && cm.index_options.class_name) || 'normal';
              var hasIndex = !!cm.index_type;
              var hasSOLRIndex = hasIndex && (class_name === 'com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex');
              var col = {name:i, type: type, index:hasIndex, solrIndex: hasSOLRIndex};
              columns[col.name] = col;
            }
            matchColumns(columns);
          } else {
            if (connection) createTable(connection);

            else that.on('ready', createTable);
          }
        });       //cql, callback, bind, options, args
        return;
      }
      if (rows.length > 0) {
        return connection.execute('select * from system.schema_columns where keyspace_name=\'' + connection.keyspace.name + '\' and columnfamily_name=\'' + name + '\'', function (err, rows) {
          var columns = {};
          rows.forEach(function (row) {
            var name = row.get('column_name').value.toLowerCase();
            if (/^_.*/.test(name) || name === 'id')
              return;
            var col = {};
            col.name = name;
            col.index = row.get('index_name').value !== null;
            var validator = row.get('validator').value;
            switch (validator) {
            case 'org.apache.cassandra.db.marshal.BytesType':
              col.type = 'blob';
              break;
            case 'org.apache.cassandra.db.marshal.BooleanType':
              col.type = 'boolean';
              break;
            case 'org.apache.cassandra.db.marshal.UTF8Type':
              col.type = ['text', 'varchar'];
              break;
            case 'org.apache.cassandra.db.marshal.FloatType':
              col.type = 'float';
              break;
            case 'org.apache.cassandra.db.marshal.DoubleType':
              col.type = 'double';
              break;
            case 'org.apache.cassandra.db.marshal.DecimalType':
              col.type = 'decimal';
              break;
            case 'org.apache.cassandra.db.marshal.DateType':
              col.type = 'timestamp';
              break;
            case 'org.apache.cassandra.db.marshal.UUIDType':
              col.type = 'uuid';
              break;
            case 'org.apache.cassandra.db.marshal.LongType':
              col.type = 'bigint';
              break;
            case 'org.apache.cassandra.db.marshal.IntegerType':
              col.type = 'varint';
              break;
            case 'org.apache.cassandra.db.marshal.Int32Type':
              col.type = 'int';
              break;
            default:
              console.log('Unknown type: ', validator);
              col.type = 'Object';
              break;
            }
            columns[col.name] = col;
          });
          matchColumns(columns);
        }, undefined, {consistencylevel:orm.ConsistencyLevel.ALL});
      } else {
        if (connection) createTable(connection);
        else that.on('ready', createTable);
      }
    }, undefined, {consistencylevel:orm.ConsistencyLevel.ALL});

    function matchColumns(columns) {
      var same = true;
      var changes = {adds:[], drops:[]};
      Object.keys(that.schema.info.fields).forEach(function (name) {
        name = name.toLowerCase();
        if (!/^_.*/.test(name) && !columns[name] && name !== 'id') {
          same = false;
          console.log(that.schema.info.fields, name);
          var type = that.schema.info.fields[name].type.name || 'Object';
          if (connection.options.logger) {
            connection.options.logger.log('Created dynamic field ' + that.cql.tableName() + '.' + name + ' ' + type);
          }
          changes.adds.push({name:name, type:type, index:that.schema.info.fields[name].index});
        }
      });
      Object.keys(columns).forEach(function (name) {
        if (!that.schema.info.fields[name]) {
          that.schema.info.fields[name] = columns[name];
          columns[name].type = {name: columns[name].type};
        }
      });
      Object.keys(columns).forEach(function (name) {
        that.schema.info.fields[name].index |= columns[name].index;
      });
        // TODO: check schema for changes and alter table
      if (same)
        createIndexes();
      else {
        async.forEachSeries(that.cql.alterTable(changes), function (cql, next) {
            connection.execute(cql, next, undefined, {consistencylevel:orm.ConsistencyLevel.ALL});
        }, createIndexes);
      }
    }

    function createTable (connection) {
      connection.execute(that.cql.createTable(), function (err) {
        if (err) {
          if (err.message.indexOf('No definition found that is not part of the PRIMARY KEY') != -1) {
            that.migrating = false;
            return safeApply(callback, [], that);
          }
        }
        createIndexes(err);
      }, undefined, {consistencylevel:orm.ConsistencyLevel.ALL});
    }

    function createIndexes (err) {
      if (err) {
        that.migrating = false;
        return safeApply(callback, [err], that);
      }
      async.forEachSeries(that.schema.getIndexes(), function (name, next) {
        if (name === "id" || name === 'solr_query' || name === '$solr')
          return next();
        if (connection.options.enableDse && !/\./.test(name)) {
          return next();
        }
        connection.execute(that.cql.createIndex(name), function (err) {
          if (err && err.name !== 'HelenusInvalidRequestException') {
            // NOTE: Not sure if HelenusInvalidRequestException only throws when the index
            //       is already defined or not, but this is to avoid an error when the index already exists
            return next(err);
          }
          next();
        }, undefined, {consistencylevel:orm.ConsistencyLevel.ALL});
      }, function (err) {
        if (err) {
          that.migrating = false;
          return callback.bind(that)(err);
        }
        if (orm.schemaSync) {
          that.connection = connection;
          orm.schemaSync(that, function (err) {
            // Drop error due to schema sync problems
            that.migrating = false;
            callback.bind(that)(null);
          });
        } else {
          that.migrating = false;
          callback.bind(that)(err);
        }
      });
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
