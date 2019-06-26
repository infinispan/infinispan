tm.begin()
cache.getAdvancedCache().lock(K)  // acquire cluster-wide lock on K
cache.put(K,V5)                   // guaranteed to succeed
tm.commit()                       // releases locks
