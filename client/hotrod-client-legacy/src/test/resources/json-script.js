// mode=local,language=javascript,datatype='application/json; charset=utf-8'
var valueJSON = {"v":"value"}
cache.put("key", JSON.stringify(valueJSON));

var value = JSON.parse(cache.get("key"))
value.v = "value2"

cache.put("key2", JSON.stringify(value))
cache.get("key2")
