package org.infinispan.spring.starter.embedded;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.actuator.InfinispanEmbeddedHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Contributes an Actuator health indicator for the embedded Cache Manager when Actuator is on the classpath.
 */
@Configuration(proxyBeanMethods = false)
@AutoConfigureAfter(InfinispanEmbeddedAutoConfiguration.class)
//Since a jar with configuration might be missing (which would result in TypeNotPresentExceptionProxy), we need to
//use String based methods.
//See https://github.com/spring-projects/spring-boot/issues/1733
@ConditionalOnClass(name = {
      "org.infinispan.manager.EmbeddedCacheManager",
      "org.springframework.boot.actuate.health.HealthIndicator"
})
@ConditionalOnProperty(value = "infinispan.embedded.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(InfinispanEmbeddedConfigurationProperties.class)
public class InfinispanEmbeddedHealthAutoConfiguration {

   public static final String HEALTH_INDICATOR_NAME = "infinispanEmbeddedHealthIndicator";

   @Bean(HEALTH_INDICATOR_NAME)
   @ConditionalOnBean(type = "org.infinispan.manager.EmbeddedCacheManager")
   @ConditionalOnMissingBean(name = HEALTH_INDICATOR_NAME)
   @ConditionalOnProperty(value = "infinispan.embedded.health.enabled", havingValue = "true", matchIfMissing = true)
   public InfinispanEmbeddedHealthIndicator infinispanEmbeddedHealthIndicator(EmbeddedCacheManager cacheManager,
                                                                             InfinispanEmbeddedConfigurationProperties properties) {
      return new InfinispanEmbeddedHealthIndicator(cacheManager, properties.getHealth());
   }
}
