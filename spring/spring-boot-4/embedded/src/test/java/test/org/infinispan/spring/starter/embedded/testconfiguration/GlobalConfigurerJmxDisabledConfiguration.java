package test.org.infinispan.spring.starter.embedded.testconfiguration;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static test.org.infinispan.spring.starter.embedded.testconfiguration.InfinispanCacheTestConfiguration.TEST_CLUSTER;

@Configuration
public class GlobalConfigurerJmxDisabledConfiguration {

   @Bean
   public InfinispanGlobalConfigurer globalConfigurer() {
      return () -> {
         final GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder()
               .transport().clusterName(TEST_CLUSTER)
               .jmx().disable()
               .build();

         return globalConfiguration;
      };
   }
}
