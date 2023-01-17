package org.infinispan.spring.starter.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
//Since a jar with configuration might be missing (which would result in TypeNotPresentExceptionProxy), we need to
//use String based methods.
//See https://github.com/spring-projects/spring-boot/issues/1733
@ConditionalOnClass(name = "org.infinispan.spring.remote.provider.SpringRemoteCacheManager")
@ConditionalOnProperty(value = "infinispan.remote.cache.enabled", havingValue = "true", matchIfMissing = true)
public class InfinispanRemoteCacheManagerAutoConfiguration {

   @Bean
   @ConditionalOnBean(RemoteCacheManager.class)
   @ConditionalOnMissingBean(type = {"org.infinispan.spring.remote.provider.SpringRemoteCacheManager", "org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean"})
   public SpringRemoteCacheManager springRemoteCacheManager(RemoteCacheManager remoteCacheManager) {
      return new SpringRemoteCacheManager(remoteCacheManager);
   }
}
