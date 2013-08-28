stepc   = require "stepc"
outcome = require "outcome"
async   = require "async"
mkdirp  = require "mkdirp"
fs      = require "fs"
type    = require "type-component"
traverse = require "traverse"

validate = require "./validate"
connect  = require "./connect"
mongodb  = require("mongodb")



###
###

module.exports = (options, next) ->

  o = outcome.e next

  stepc.async(

    # validate fields
    (() -> 
      validate(options, @)
    ),

    # make the directory
    o.s(() ->
      try
        mkdirp.sync options.path
      catch e
      @()
    ),

    # connect to mongodb
    o.s(() ->   
      connect(options, @)
    ),

    # get all the collection names
    o.s((@db) -> 
      db.collectionNames @
    ),


    # start the export process
    o.s((names) ->

      collections = names.map (data) => 

        @db.collection data.name.split(".").slice(1).join(".")

      exportCollections options, collections, @
    ),

    # done
    next
  )

###
###

exportCollections = (options, collections, next) ->
  loadCollections collections, (err, collections) ->
    mapItemRelationships collections
    async.eachSeries collections, exportCollection(options), next


###
###

mapItemRelationships = (collections) ->

  console.log("mapping item relationships")
  
  all  = {}

  # first grab all the items
  for collection in collections
    for item in collection.items
      item.__collection = collection.name
      item.__refs = {}
      all[item._id] = item

  # create 
  for key of all
    item = all[key]
    attachRefs(item, all)





attachRefs = (item, all) ->

  keys = []


  traverse(item).forEach (x) ->
    keys = []

    p = this


    while p
      if isNaN(p.key) and p.key
        keys.unshift p.key
      p = p.parent

    key = keys.join(".")


    if ref = all[x]
      refs = ref.__refs

      unless r = refs[item.__collection]
        r = refs[item.__collection] = []

      unless ~r.indexOf(key)
        r.push key

    if x and typeof x is "object" and not /^(Array|Object)$/.test x.constructor.name
      this.update { __type: x.constructor.name, value: x }
      this.block()


###
###

attachRefs2 = (item, all, current, keys = []) ->

  unless current
    current = item
  
  for key of current

    value = current[key]

    if (ref = all[value])
      kp = keys.concat(key).join(".")

      refs = ref.__refs


      unless r = refs[item.__collection]
        r = refs[item.__collection] = []

      unless ~r.indexOf(kp)
        r.push kp

    else if (t = type(value)) is "array"
      for sub in value
        if sub
          attachRefs(item, all, sub, keys.concat(key))
    else if t is "object" and value 
      attachRefs(item, all, value, keys.concat())

    if value and (typeof value is "object") and not /^Array|Object$/.test value.constructor.name
      current[key] = { __type: value.constructor.name, value: value }


  refs


###
###

loadCollections = (collections, next) ->
  
  data = []

  async.eachSeries collections, ((collection, next) ->
    collection.find().toArray (err, result) ->
      return next(err) if err?
      console.log "loaded %s (%d)", collection.collectionName, result.length
      data.push { name: collection.collectionName, items: result }
      next()
  ), (err) ->
    return next(err) if err?
    next null, data
  


###
###

exportCollection = (options) ->
  (collection, next) ->

    path = options.path + "/" + collection.name + ".json"

    console.log("exporting %s", collection.name);

    o = outcome.e next

    stepc.async(

      # save
      o.s((results) ->
        fs.writeFile(path, JSON.stringify(collection.items, null, 2), @)
      ),

      # done
      next
    )


  