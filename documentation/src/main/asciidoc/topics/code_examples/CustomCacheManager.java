public class Config {

   @GreetingCache
   @Produces
   @ApplicationScoped
   public EmbeddedCacheManager specificEmbeddedCacheManager() {
      return new DefaultCacheManager(new ConfigurationBuilder()
                                          .expiration()
                                              .lifespan(60000l)
                                          .build());
   }

   @RemoteGreetingCache
   @Produces
   @ApplicationScoped
   public RemoteCacheManager specificRemoteCacheManager() {
       return new RemoteCacheManager("localhost", 1544);
   }
}
