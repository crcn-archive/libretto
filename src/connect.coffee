mongodb = require("mongodb")
client  = mongodb.MongoClient
step    = require "step"
memoize = require "memoizee"
_log = require "./log"

###
 connects to the database
###

connect = memoize(((db, next) ->
  # assume it's a local db if mongodb:// isn't present
  unless ~db.indexOf("mongodb://")
    db = "mongodb://127.0.0.1:27017/" + db

  _log("connecting to %s", db)

  client.connect db, next
), { async: true })

  

module.exports = (options, next) ->
  connect options.db, next

