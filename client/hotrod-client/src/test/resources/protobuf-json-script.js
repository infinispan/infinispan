// mode=local,language=javascript,datatype='application/json;type=java.lang.String'

// Obtain an existent user
var clone = JSON.parse(cache.get("John Doe"));

// Change some attributes
clone.id = 10
clone.name = "Rex"
clone.age = 67

// Insert under a new key
cache.put("Rex", JSON.stringify(clone))

// Return the new user
cache.get("Rex")
