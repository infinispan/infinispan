package test.infinispan.integration.embedded;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.spring.starter.embedded.InfinispanCacheConfigurer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableCaching
@RestController
public class SimpleApp {
   public static void main(String[] args) {
      SpringApplication.run(SimpleApp.class, args);
   }

   @GetMapping("/hello")
   public String doSomething() {
      return "hello";
   }

   @Bean
   public InfinispanCacheConfigurer simpleCacheConfigurer() {
      return manager -> {
         final Configuration ispnConfig = new ConfigurationBuilder()
                 .clustering()
                 .cacheMode(CacheMode.LOCAL)
                 .statistics().enable()
                 .build();

         manager.defineConfiguration("simpleCache", ispnConfig);
      };
   }
}
