mongodb = require("mongodb")
client  = mongodb.MongoClient
step    = require "step"

###
 connects to the database
###

module.exports = (options, next) ->
  
  # assume it's a local db if mongodb:// isn't present
  unless ~options.db.indexOf("mongodb://")
    options.db = "mongodb://127.0.0.1:27017/" + options.db

  console.log("connecting to %s", options.db)

  client.connect options.db, next