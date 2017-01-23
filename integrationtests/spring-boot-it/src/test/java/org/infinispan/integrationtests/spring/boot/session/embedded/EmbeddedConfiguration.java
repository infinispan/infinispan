package org.infinispan.integrationtests.spring.boot.session.embedded;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.integrationtests.spring.boot.session.configuration.WebConfig;
import org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean;
import org.infinispan.spring.session.configuration.EnableInfinispanEmbeddedHttpSession;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableAutoConfiguration
@EnableInfinispanEmbeddedHttpSession
@Import(WebConfig.class)
public class EmbeddedConfiguration {

   @Bean
   public SpringEmbeddedCacheManagerFactoryBean springCacheManager() {
      SpringEmbeddedCacheManagerFactoryBean cacheManagerFactoryBean = new SpringEmbeddedCacheManagerFactoryBean();
      cacheManagerFactoryBean.addCustomGlobalConfiguration(new GlobalConfigurationBuilder().defaultCacheName("sessions"));
      return cacheManagerFactoryBean;
   }

}
