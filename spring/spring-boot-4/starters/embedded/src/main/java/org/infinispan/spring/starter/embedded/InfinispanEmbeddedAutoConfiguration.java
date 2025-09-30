package org.infinispan.spring.starter.embedded;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.cache.autoconfigure.CacheAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
@AutoConfigureBefore(CacheAutoConfiguration.class)
//Since a jar with configuration might be missing (which would result in TypeNotPresentExceptionProxy), we need to
//use String based methods.
//See https://github.com/spring-projects/spring-boot/issues/1733
@ConditionalOnClass(name = "org.infinispan.manager.EmbeddedCacheManager")
@ConditionalOnProperty(value = "infinispan.embedded.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(InfinispanEmbeddedConfigurationProperties.class)
public class InfinispanEmbeddedAutoConfiguration {

   public static final String DEFAULT_CACHE_MANAGER_QUALIFIER = "defaultCacheManager";

   @Autowired
   private InfinispanEmbeddedConfigurationProperties infinispanProperties;

   @Autowired(required = false)
   private final List<InfinispanCacheConfigurer> configurers = Collections.emptyList();

   @Autowired(required = false)
   private final Map<String, org.infinispan.configuration.cache.Configuration> cacheConfigurations = Collections.emptyMap();

   @Autowired(required = false)
   private InfinispanGlobalConfigurer infinispanGlobalConfigurer;

   @Autowired(required = false)
   private final List<InfinispanGlobalConfigurationCustomizer> globalConfigurationCustomizers = Collections.emptyList();

   @Bean(destroyMethod = "stop")
   @Conditional(InfinispanEmbeddedCacheManagerChecker.class)
   @ConditionalOnMissingBean
   @Qualifier(DEFAULT_CACHE_MANAGER_QUALIFIER)
   public DefaultCacheManager defaultCacheManager() throws IOException {
      final String configXml = infinispanProperties.getConfigXml();
      final DefaultCacheManager manager;

      ConfigurationBuilderHolder holder;
      if (!configXml.isEmpty()) {
         holder = new ParserRegistry().parseFile(configXml);
         GlobalConfigurationBuilder globalConfigurationBuilder = holder.getGlobalConfigurationBuilder();
         if (globalConfigurationBuilder.serialization().getMarshaller() == null) {
            // spring session needs does not work with protostream right now, easy users to configure the marshaller
            // and the classes we need for spring embedded
            globalConfigurationBuilder.serialization().marshaller(new JavaSerializationMarshaller());
         }
         globalConfigurationCustomizers.forEach(customizer -> customizer.customize(globalConfigurationBuilder));
         allowInternalClasses(globalConfigurationBuilder);
         manager = new DefaultCacheManager(holder, false);
      } else {
         GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
         globalConfigurationBuilder.serialization().marshaller(new JavaSerializationMarshaller());
         allowInternalClasses(globalConfigurationBuilder);

         if (infinispanGlobalConfigurer != null) {
            globalConfigurationBuilder.read(infinispanGlobalConfigurer.getGlobalConfiguration());
         } else {
            globalConfigurationBuilder.transport().clusterName(infinispanProperties.getClusterName());
            globalConfigurationBuilder.jmx().enable();
         }

         globalConfigurationCustomizers.forEach(customizer -> customizer.customize(globalConfigurationBuilder));
         manager = new DefaultCacheManager(globalConfigurationBuilder.build(), false);
      }

      cacheConfigurations.forEach(manager::defineConfiguration);
      configurers.forEach(configurer -> configurer.configureCache(manager));

      manager.start();
      return manager;
   }

   private void allowInternalClasses(GlobalConfigurationBuilder globalConfigurationBuilder) {
      globalConfigurationBuilder.serialization().allowList().addClass("org.springframework.session.MapSession");
      globalConfigurationBuilder.serialization().allowList().addRegexp("java.util.*");
   }
}
