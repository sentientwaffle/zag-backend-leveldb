var level      = require('level-party')
  , Stream     = require('stream')
  , inherits   = require('util').inherits
  , reNotFound = /^Key not found/

module.exports = LevelBackend

function noop() {}

var fakeAgent = {histogram: noop, counter: noop}

///
/// options -
///   env   - String
///   db    - String path to leveldb, or a leveldb instance
///           (must have encoding:json).
///   agent - MetricsAgent (optional)
///   onError(err)
///
function LevelBackend(options) {
  this.level   = typeof options.db === "string"
               ? level(options.db, {valueEncoding: "json"})
               : options.db
  this.env     = options.env
  this.agent   = options.agent   || fakeAgent
  this.onError = options.onError || noop

  this.tData       = this.env + "_data:"
  this.tKeys       = this.env + "_keys:"
  this.tTags       = this.env + "_tags:"
  this.tRules      = this.env + "_rules:"
  this.tDashboards = this.env + "_dashboards:"
  this.tTagTypes   = this.env + "_tagtypes:"

  var _this = this
  this._onKeyInsert = function(err) { _this.onKeyInsert(err) }
}

LevelBackend.prototype.setup = function(cb) { process.nextTick(cb) }

LevelBackend.prototype.close = function() { this.level.close() }

// mkey  - String
// start - Integer timestamp (ms)
// end   - Integer timestamp (ms)
// callback(err, [point])
LevelBackend.prototype.getPoints = function(mkey, start, end, callback) {
  this.level.createReadStream(
  { start:  this.getPointKey(mkey, start)
  , end:    this.getPointKey(mkey, end)
  , keys:   false
  , values: true
  }).pipe(new LevelRowCollector(callback))
}

// points - { mkey : point }
LevelBackend.prototype.savePoints = function(points) {
  var mkeys = Object.keys(points)
    , tKeys = this.tKeys

  for (var i = 0; i < mkeys.length; i++) {
    var mkey = mkeys[i]
      , pt   = points[mkey]
      , type = identify(pt)
    this._put(tKeys + mkey, {type: type}, this._onKeyInsert)
    this.savePoint(mkey, pt, this._onKeyInsert)
  }
}

function identify(pt) {
  return pt.mean !== undefined ? "histogram"
       : pt.data !== undefined ? "llq"
       : "counter"
}

LevelBackend.prototype.savePoint = function(mkey, point, callback) {
  this._put(this.getPointKey(mkey, point.ts), point, callback)
}

LevelBackend.prototype.onKeyInsert = function(err) {
  if (err) this.onError(err)
}

LevelBackend.prototype.getMetricsKeys = function(callback) {
  this.level.createReadStream(
  { start: this.tKeys
  , end:   this.tKeys + "~"
  }).pipe(new LevelRowCollector(callback, kvToRow))
}

function kvToRow(row) {
  return { key:  stripTable(row.key)
         , type: row.value.type
         }
}

function stripTable(key) { return key.slice(key.indexOf(":") + 1) }

LevelBackend.prototype.deleteMetricsKey = function(mkey, callback) {
  this._del(this.tKeys + mkey, callback)
}

///
/// Tags
///

// begin - Integer timestamp (ms)
// end   - Integer timestamp (ms)
LevelBackend.prototype.getTagRange = function(begin, end, callback) {
  this.level.createReadStream(
  { start:  this.tTags + begin
  , end:    this.tTags + end + "~"
  , keys:   false
  , values: true
  }).pipe(new LevelRowCollector(callback))
}

// tag - {ts, label, color[, id]}
LevelBackend.prototype.setTag = function(tag, callback) {
  var id = tag.id = tag.id || (tag.ts + "_" + digits(3))
  this._put(this.tTags + id, tag, callback)
}

LevelBackend.prototype.deleteTag = function(tagID, callback) {
  this._del(this.tTags + tagID, callback)
}

function digits(n) {
  return Math.floor(Math.random() * Math.pow(10, n))
}

///
/// Tag types
///

LevelBackend.prototype.getTagTypes = function(callback) {
  this.level.createReadStream(
  { start:  this.tTagTypes
  , end:    this.tTagTypes + "~"
  , keys:   false
  , values: true
  }).pipe(new LevelRowCollector(callback))
}

// typeOpts - {color, name}
// callback(err)
LevelBackend.prototype.createTagType = function(typeOpts, callback) {
  var id = typeOpts.id = Date.now() + "_" + digits(3)
  this._put(this.tTagTypes + id, typeOpts, callback)
}

LevelBackend.prototype.deleteTagType = function(typeID, callback) {
  this._del(this.tTagTypes + typeID, callback)
}

///
/// Rules
///

LevelBackend.prototype.getRule = function(mkey, callback) {
  this._get(this.tRules + mkey, callback)
}

LevelBackend.prototype.setRule = function(mkey, rule, callback) {
  this._put(this.tRules + mkey, rule, callback)
}

///
/// Dashboards
///

LevelBackend.prototype.getDashboard = function(id, callback) {
  this._get(this.tDashboards + id, callback)
}

LevelBackend.prototype.setDashboard = function(id, dashboard, callback) {
  this._put(this.tDashboards + id, dashboard, callback)
}

LevelBackend.prototype.deleteDashboard = function(id, callback) {
  this._del(this.tDashboards + id, callback)
}

// callback(err, ids)
LevelBackend.prototype.listDashboards = function(callback) {
  this.level.createReadStream(
  { start:  this.tDashboards
  , end:    this.tDashboards + "~"
  , keys:   true
  , values: false
  }).pipe(new LevelRowCollector(callback, stripTable))
}

////////////////////////////////////////////////////////////////////////////////

LevelBackend.prototype.getPointKey = function(mkey, minute) {
  return this.tData + mkey + "||" + minute
}

LevelBackend.prototype._get = function(key, callback) {
  this.level.get(key, this._track("get", key, callback))
}

LevelBackend.prototype._put = function(key, value, callback) {
  this.level.put(key, value, this._track("put", key, callback))
}

LevelBackend.prototype._del = function(key, callback) {
  this.level.del(key, this._track("del", key, callback))
}

LevelBackend.prototype._track = function(op, key, callback) {
  var bucket = key.slice(0, key.indexOf(":"))
    , agent  = this.agent
    , start  = Date.now()
  return function(err, value) {
    agent.histogram("timing|" + op + "|" + bucket, Date.now() - start)
    if (err && reNotFound.test(err.message)) {
      err = null
    }
    if (err) {
      agent.counter("error|" + op + "|" + bucket)
    }
    callback(err, value)
  }
}

////////////////////////////////////////////////////////////////////////////////

// callback(err, rows)
// write(key, value) -> row
function LevelRowCollector(callback, write) {
  this.readable = false
  this.writable = true
  this.callback = callback
  this.rows     = []
  this._write   = write
}

inherits(LevelRowCollector, Stream)

LevelRowCollector.prototype.done = function(err, rows) {
  if (this.callback) this.callback(err, rows)
  this.callback = this.rows = null
  this.writable = false
}

LevelRowCollector.prototype.write = function(obj) {
  this.rows.push(this._write ? this._write(obj) : obj)
}

LevelRowCollector.prototype.end = function() { this.done(null, this.rows) }

LevelRowCollector.prototype.destroy = function() {
  this.done(new Error("leveldb row scan error"))
}
