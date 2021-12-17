@EnableInfinispanEmbeddedHttpSession
@Configuration
public class Config {

   @Bean
   public SpringEmbeddedCacheManagerFactoryBean springCacheManager() {
      return new SpringEmbeddedCacheManagerFactoryBean();
   }

   //An optional configuration bean responsible for replacing the default
   //cookie that obtains configuration.
   //For more information refer to the Spring Session documentation.
   @Bean
   public HttpSessionIdResolver httpSessionIdResolver() {
       return HeaderHttpSessionIdResolver.xAuthToken();
   }
}
