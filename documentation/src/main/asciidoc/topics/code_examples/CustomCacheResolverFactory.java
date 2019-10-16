@CacheDefaults(cacheResolverFactory = CustomCacheResolverFactory.class)
public class GreetingService {
    @CacheResult
    public String greet(String user) {
        return "Hello" + user;
    }
}

@ApplicationScoped
class CustomCacheResolverFactory {
   @GreetingCache
   @Inject
   CacheManager cacheManager;

   @Override
   public CacheResolver getCacheResolver(CacheMethodDetails<? extends Annotation> cacheMethodDetails) {
      return new CustomCacheResolver(cacheManager);
   }

   @Override
   public CacheResolver getExceptionCacheResolver(CacheMethodDetails<CacheResult> cacheMethodDetails) {
      return new CustomCacheResolver(cacheManager);
   }
}

public class DefaultCacheResolver implements CacheResolver {
   private CacheManager cacheManager;

   DefaultCacheResolver(CacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      String cacheName = cacheInvocationContext.getCacheName();
      return getOrCreateCache(cacheName);
   }

   private synchronized <K, V> Cache<K, V> getOrCreateCache(String cacheName) {
      Cache<K, V> cache = cacheManager.getCache(cacheName);
      if (cache != null)
         return cache;

      return cacheManager.createCache(cacheName, new javax.cache.configuration.MutableConfiguration<>());
   }
}
