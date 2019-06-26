cache.get("A") // returns 1
cache.get("B") // returns 1

Thread1: tx1.begin()
Thread1: cache.put("A", 2)
Thread1: cache.put("B", 2)
Thread2:                                       tx2.begin()
Thread2:                                       cache.get("A") // returns 1
Thread1: tx1.commit()
Thread2:                                       cache.get("B") // returns 2
Thread2:                                       tx2.commit()
