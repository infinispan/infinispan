package org.infinispan.spring.starter.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
//Since a jar with configuration might be missing (which would result in TypeNotPresentExceptionProxy), we need to
//use String based methods.
//See https://github.com/spring-projects/spring-boot/issues/1733
@ConditionalOnClass(name = "org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager")
@ConditionalOnProperty(value = "infinispan.embedded.cache.enabled", havingValue = "true", matchIfMissing = true)
public class InfinispanEmbeddedCacheManagerAutoConfiguration {

   @Autowired
   private ApplicationContext context;

   @Bean
   @ConditionalOnMissingBean(type = {"org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager", "org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManagerFactoryBean"})
   @ConditionalOnBean(type = "org.infinispan.manager.EmbeddedCacheManager")
   public SpringEmbeddedCacheManager springEmbeddedCacheManager(EmbeddedCacheManager embeddedCacheManager) {
      InfinispanEmbeddedConfigurationProperties infinispanProperties = context.getBean(InfinispanEmbeddedConfigurationProperties.class);
      return new SpringEmbeddedCacheManager(embeddedCacheManager, infinispanProperties == null? false : infinispanProperties.isReactive());
   }
}
