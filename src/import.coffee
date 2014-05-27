stepc    = require "stepc"
outcome  = require "outcome"
async    = require "async"
mkdirp   = require "mkdirp"
glob     = require "glob"
fs       = require "fs"
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
      loadData options, @
    )

    #
    o.s((data) ->
      importFixtures data, @db, @
    ),

    #
    next
  )


loadData = (options, next) ->

  if options.data
    return next(null, options.data)


  collectionFiles = glob.sync(options.path).filter (name) ->
    not /.DS_Store/.test(name) and /(json|js)$/.test(name)  


  data = []

  collectionFiles.forEach (fp) ->
    data = data.concat require fp


  next null, data

###
###

importFixtures = (data, db, next) ->

  o = outcome.e next

  stepc.async(
    (() ->
      loadFixtures data, @
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


loadFixtures = (items, next) ->

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
        _log err
      next()
    )
  ), next

  (fixturePath, next) ->
    collectionName = path.basename(fixturePath).split(".").shift()
    _log "importing %s", collectionName

    results = require(fixturePath)
    importItems results, db, next

