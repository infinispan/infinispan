@EnableCaching
@Configuration
public class Config {

   @Bean
   public CacheManager cacheManager() {
      return new SpringEmbeddedCacheManager(infinispanCacheManager());
   }

   private EmbeddedCacheManager infinispanCacheManager() {
      return new DefaultCacheManager();
   }

}
