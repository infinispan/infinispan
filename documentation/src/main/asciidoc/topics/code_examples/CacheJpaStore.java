Cache<String, User> usersCache = cacheManager.getCache("myJPACache"); // configured for User entity class
usersCache.put("raytsang", new User());
Cache<Integer, Teacher> teachersCache = cacheManager.getCache("myJPACache"); // cannot do this when this cache is configured to use a JPA cache store
teachersCache.put(1, new Teacher());
