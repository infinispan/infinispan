package test.org.infinispan.spring.starter.remote.testconfiguration;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.spring.starter.remote.InfinispanRemoteConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfinispanCacheTestConfiguration {

   public static final int PORT = 11555;

   @Bean
   public InfinispanRemoteConfigurer infinispanRemoteConfigurer() {
      return () -> new ConfigurationBuilder().statistics().disable().addServer().host("127.0.0.1").port(PORT).build();
   }
}
