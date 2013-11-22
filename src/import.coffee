stepc   = require "stepc"
outcome = require "outcome"
async   = require "async"
mkdirp  = require "mkdirp"
fs      = require "fs"
readline = require "readline"
path     = require "path"
traverse = require "traverse"

validate  = require "./validate"
connect   = require "./connect"
ObjectID = require("mongodb").ObjectID
_log = require "./log"


_types = {
  ObjectID: ObjectID,
  Date: Date
}


###
###

module.exports = (options, next) ->

  o  = outcome.e(next)

  # yuck
  process.env.LOG_LIBRETTO = options.verbose ? process.env.LOG_LIBRETTO
  
  stepc.async(

    # validate fields
    (() -> 
      validate(options, @)
    ),

    # connect to mongodb
    o.s(() ->   
      connect({ db: options.db }, @)
    ),

    #import the collections
    o.s((@db) ->
      @()
    ),

    # load the collection paths
    o.s(() ->
      fs.readdir options.path, @
    )

    #
    o.s((collectionFiles) ->

      collectionFiles = collectionFiles.filter((name) ->
        not /.DS_Store/.test name
      ).map (name) ->
        options.path + "/" + name

      importFixtures collectionFiles, @db, @
    ),

    #
    next
  )

###
###

importFixtures = (fixturePaths, db, next) ->

  o = outcome.e next

  stepc.async(
    (() ->
      loadFixtures fixturePaths, @
    ),
    o.s((@items) ->
      removeReferences db, items, @
    ),
    o.s(() ->
      removeExplicit db, @items, @
    ),
    o.s(() ->
      insertItems db, @items, @
    ),
    next
  )


loadFixtures = (fixturePaths, next) ->
  
  items = []

  async.eachSeries fixturePaths, ((fixturePath, next) ->
    items = items.concat require(fixturePath)
    next()
  ), outcome.e(next).s () ->
    next null, items.map (item) ->
      # fix the object type
      traverse(item.data).forEach (x) ->
        if x and x.__type
          this.update(new _types[x.__type](x.value))
      item

removeExplicit = (db, items, next) ->
  rm = items.filter (item) -> item.method is "remove"
  async.eachSeries rm, ((item, next) ->
    _log("remove %s %s", item.collection, JSON.stringify(item.query));
    db.collection(item.collection).remove(item.query, next);
  ), next


removeReferences = (db, items, next) ->
  async.eachSeries items, (item, next) ->

    refs = []

    for collection of item.refs
      keys = item.refs[collection]
      refs.push keys.map((key) -> { collection: collection, field: key })...

    async.eachSeries refs, ((ref, next) ->
      search = {}

      # object id might be a string, or object id instance
      search[ref.field] = item.data._id
      _log("remove %s:%s.%s", ref.collection, item.data._id, ref.field);
      db.collection(ref.collection).remove(search, next)
    ), next
  , next



###
###

insertItems = (db, items, next) ->
  
  items = items.filter (item) -> item.method is "insert"

  async.eachSeries items, ((item, next) ->

    return next() if /^system/.test item.collection


    _log("insert %s:%s", item.collection, item.data._id)
    db.collection(item.collection).insert(item.data, (err) ->
      if err
        console.warn err
      next()
    )
  ), next

  (fixturePath, next) ->
    collectionName = path.basename(fixturePath).split(".").shift()
    _log "importing %s", collectionName

    results = require(fixturePath)
    importItems results, db, next

