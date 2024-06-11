// Obtain a transaction Manager
var tm = cache.getTransactionManager()

// Begin a transaction
var tx = tm.begin()
cache.put("parameter", parameter)
tm.commit()

// Return the size of the cache
cache.size()
