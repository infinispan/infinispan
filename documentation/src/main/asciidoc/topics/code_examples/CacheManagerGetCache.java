cacheManager.getCache("testCache").put("testKey", "testValue");
System.out.println("Received value from cache: " + cacheManager.getCache("testCache").get("testKey"));
