verify = require "verify"

###
 validates options for import / export
###

module.exports = (options, next) ->
  return unless verify().that(options).onError(next).has("path", "db").success
  options.path = options.path.replace("~", process.env.HOME);
  next()