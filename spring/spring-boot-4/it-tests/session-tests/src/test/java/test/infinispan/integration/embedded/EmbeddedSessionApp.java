package test.infinispan.integration.embedded;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.spring.embedded.session.configuration.EnableInfinispanEmbeddedHttpSession;
import org.infinispan.spring.starter.embedded.InfinispanCacheConfigurer;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import test.infinispan.integration.SecurityConfig;

@SpringBootApplication
@EnableCaching
@EnableInfinispanEmbeddedHttpSession
@EnableWebSecurity
@Import(SecurityConfig.class)
public class EmbeddedSessionApp {
   @Bean
   public InfinispanCacheConfigurer cacheConfigurer() {
      return manager -> {
         final org.infinispan.configuration.cache.Configuration ispnConfig = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.DIST_SYNC)
               .encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
               .statistics().disable()
               .build();


         manager.defineConfiguration("sessions", ispnConfig);
      };
   }

   @Bean
   public InfinispanGlobalConfigurer globalCustomizer() {
      return () -> GlobalConfigurationBuilder
            .defaultClusteredBuilder()
            .metrics().gauges(false).histograms(false)
            .globalState().disable()
            .build();
   }
}
