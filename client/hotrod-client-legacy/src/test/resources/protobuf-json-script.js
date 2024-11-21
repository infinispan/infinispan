// mode=local,language=javascript,datatype='application/json;type=java.lang.String'

// Obtain an existent user
var clone = JSON.parse(cache.get(1))

// Change some attributes
clone.id = 3
clone.name = "Rex"
clone.age = 67

// Insert under a new key
cache.put(3, JSON.stringify(clone))

// Return the new user
cache.get(3)
