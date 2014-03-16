var tap     = require('tap')
  , test    = tap.test
  , fs      = require('fs')
  , path    = require('path')
  , Backend = require('..')
  , level   = require('level')
  , dbPath  = __dirname + "/test.db"
  , db      = level(dbPath, {encoding: "json"})
  , env     = 0

tap.on("end", function() {
  db.close()
  var files = fs.readdirSync(dbPath)
  for (var i = 0; i < files.length; i++) {
    fs.unlinkSync(path.join(dbPath, files[i]))
  }
  fs.rmdirSync(dbPath)
})

test("Backend#getPoints none", function(t) {
  var backend = makeBackend()
  backend.getPoints("foo", 0, 100, function(err, points) {
    if (err) throw err
    t.deepEquals(points, [])
    t.end()
  })
})

test("Backend#savePoints, Backend#getPoints", function(t) {
  var backend = makeBackend()
  backend.savePoint("foo", {ts: 20, count: 64}, function(err) {
    if (err) throw err
    backend.savePoint("foo", {ts: 10, count: 32}, function(err) {
      if (err) throw err
      backend.getPoints("foo", 0, 99, function(err, points) {
        if (err) throw err
        t.deepEquals(points, [{ts: 10, count: 32}, {ts: 20, count: 64}])
        t.end()
      })
    })
  })
})

test("Backend#savePoints, Backend#getMetricsKeys", function(t) {
  var backend = makeBackend()
  backend.savePoints(
  { foo: {ts: 10, count: 2}
  , bar: {ts: 20, count: 4}
  })
  setTimeout(checkPoints, 10)

  function checkPoints() {
    backend.getPoints("foo", 0, 30, function(err, points) {
      if (err) throw err
      t.deepEquals(points, [{ts: 10, count: 2}])
      backend.getPoints("bar", 0, 30, function(err, points) {
        if (err) throw err
        t.deepEquals(points, [{ts: 20, count: 4}])
        checkKeys()
      })
    })
  }

  function checkKeys() {
    backend.getMetricsKeys(function(err, keys) {
      if (err) throw err
      t.deepEquals(keys,
        [ {key: "bar", type: "counter"}
        , {key: "foo", type: "counter"}
        ])
      t.end()
    })
  }
})

test("Backend#deleteMetricsKey", function(t) {
  var backend = makeBackend()
  backend.savePoints(
  { foo: {ts: 10, count: 2}
  , bar: {ts: 20, count: 4}
  })
  setTimeout(deleteKey, 10)

  function deleteKey() {
    backend.deleteMetricsKey("bar", function(err) {
      if (err) throw err
      backend.getMetricsKeys(function(err, keys) {
        if (err) throw err
        t.deepEquals(keys, [{key: "foo", type: "counter"}])
        t.end()
      })
    })
  }
})

test("Backend#getTagRange empty", function(t) {
  var backend = makeBackend()
  backend.getTagRange(1, 30, function(err, tags) {
    if (err) throw err
    t.deepEquals(tags, [])
    t.end()
  })
})

test("Backend#getTagRange, Backend#setTag", function(t) {
  var backend = makeBackend()
  backend.setTag({ts: 14, label: "goodbye", color: "#ff0"}, function(err) {
    if (err) throw err
    backend.setTag({ts: 11, label: "hello", color: "#ff0"}, function(err) {
      if (err) throw err
      backend.getTagRange(1, 30, function(err, tags) {
        if (err) throw err
        t.notEquals(tags[0].id, tags[1].id)
        t.deepEquals(tags,
          [ {ts: 11, label: "hello",   color: "#ff0", id: tags[0].id}
          , {ts: 14, label: "goodbye", color: "#ff0", id: tags[1].id}
          ])
        t.end()
      })
    })
  })
})

test("Backend#deleteTag", function(t) {
  var backend = makeBackend()
  backend.setTag({ts: 14, label: "goodbye", color: "#ff0"}, function(err) {
    if (err) throw err
    getTags(function(err, tags) {
      if (err) throw err
      backend.deleteTag(tags[0].id, function(err) {
        if (err) throw err
        getTags(function(err, tags) {
          if (err) throw err
          t.deepEquals(tags, [])
          t.end()
        })
      })
    })
  })
  function getTags(callback) { backend.getTagRange(1, 30, callback) }
})

test("Backend#listDashboards empty", function(t) {
  var backend = makeBackend()
  backend.listDashboards(function(err, dashIDs) {
    if (err) throw err
    t.deepEquals(dashIDs, [])
    t.end()
  })
})

test("Backend#listDashboards not empty", function(t) {
  var backend = makeBackend()
  backend.setDashboard("foo", {id: "foo", graphs: {}}, function(err) {
    if (err) throw err
    backend.listDashboards(function(err, dashIDs) {
      if (err) throw err
      t.deepEquals(dashIDs, ["foo"])
      t.end()
    })
  })
})


////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

function makeBackend() {
  return new Backend({db: db, env: "env_" + (++env)})
}
