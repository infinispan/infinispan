try (InfinispanContainer container = new InfinispanContainer()) {
   container.start();
   try (RemoteCacheManager cacheManager = new RemoteCacheManager(container.getConnectionURI()))) {
      // Use the RemoteCacheManager
   }
}
