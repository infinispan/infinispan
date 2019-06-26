tm.begin()
cache.put(K,V)    // acquire cluster-wide lock on K
cache.put(K2,V2)  // acquire cluster-wide lock on K2
cache.put(K,V5)   // no-op, we already own cluster wide lock for K
tm.commit()       // releases locks
