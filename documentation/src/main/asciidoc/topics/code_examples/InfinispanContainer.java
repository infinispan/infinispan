try (InfinispanContainer container = new InfinispanContainer()) {
   container.start();
   try (RemoteCacheManager cacheManager = container.getRemoteCacheManager()) {
      // Use the RemoteCacheManager
   }
}
