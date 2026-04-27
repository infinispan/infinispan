package org.infinispan.spring.starter.remote;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.spring.remote.provider.SchemaRegistration;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.core.io.Resource;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

@Configuration
@ComponentScan
@AutoConfigureBefore(CacheAutoConfiguration.class)
//Since a jar with configuration might be missing (which would result in TypeNotPresentExceptionProxy), we need to
//use String based methods.
//See https://github.com/spring-projects/spring-boot/issues/1733
@ConditionalOnClass(name = "org.infinispan.client.hotrod.RemoteCacheManager")
@ConditionalOnProperty(value = "infinispan.remote.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(InfinispanRemoteConfigurationProperties.class)
public class InfinispanRemoteAutoConfiguration {

   private static final Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass().getName());

   public static final String REMOTE_CACHE_MANAGER_BEAN_QUALIFIER = "remoteCacheManager";

   @Autowired
   private InfinispanRemoteConfigurationProperties infinispanProperties;

   @Autowired(required = false)
   private InfinispanRemoteConfigurer infinispanRemoteConfigurer;

   @Autowired(required = false)
   private org.infinispan.client.hotrod.configuration.Configuration infinispanConfiguration;

   @Autowired(required = false)
   private final List<InfinispanRemoteCacheCustomizer> cacheCustomizers = Collections.emptyList();

   @Autowired
   private ApplicationContext ctx;

   @Autowired(required = false)
   private List<SerializationContextInitializer> contextInitializers;

   @Bean
   @Conditional({ConditionalOnCacheType.class, ConditionalOnConfiguration.class})
   @ConditionalOnMissingBean
   @Qualifier(REMOTE_CACHE_MANAGER_BEAN_QUALIFIER)
   public RemoteCacheManager remoteCacheManager() throws IOException {

      boolean hasHotRodPropertiesFile = ctx.getResource(infinispanProperties.getClientProperties()).exists();
      boolean hasConfigurer = infinispanRemoteConfigurer != null;
      boolean hasProperties = StringUtils.hasText(infinispanProperties.getServerList());
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //by default, add java white list and marshaller
      builder.addJavaSerialAllowList("java.util.*", "java.time.*", "org.springframework.*", "org.infinispan.spring.common.*", "org.infinispan.spring.remote.*");
      builder.marshaller(new JavaSerializationMarshaller());

      if (hasConfigurer) {
         builder.read(Objects.requireNonNull(infinispanRemoteConfigurer.getRemoteConfiguration()));
      } else if (hasHotRodPropertiesFile) {
         String remoteClientPropertiesLocation = infinispanProperties.getClientProperties();
         Resource hotRodClientPropertiesFile = ctx.getResource(remoteClientPropertiesLocation);
         try (InputStream stream = hotRodClientPropertiesFile.getURL().openStream()) {
            Properties hotrodClientProperties = new Properties();
            hotrodClientProperties.load(stream);
            builder.withProperties(hotrodClientProperties);
         }
      } else if (hasProperties) {
         builder.withProperties(infinispanProperties.getProperties());
      } else if (infinispanConfiguration != null) {
         builder.read(infinispanConfiguration);

      } else {
         throw new IllegalStateException("Not enough data to create RemoteCacheManager. Check InfinispanRemoteCacheManagerChecker" +
               "and update conditions.");
      }

      // Auto discover and register schemas
      List<GeneratedSchema> discoveredSchemas = new ArrayList<>();
      if (infinispanProperties.isUseSchemaRegistration()) {
         // Classpath scanning for GeneratedSchema implementations
         try {
            List<String> packages = AutoConfigurationPackages.get((BeanFactory) ctx);
            discoveredSchemas = SchemaRegistration.discoverSchemas(
                  ctx.getClassLoader(), packages);
         } catch (IllegalStateException e) {
            logger.fine("Auto-configuration packages not available, skipping schema classpath scanning");
         }

         // Collect user defined SerializationContextInitializer beans
         Set<String> registeredClasses = new HashSet<>();
         if (contextInitializers != null) {
            for (SerializationContextInitializer sci : contextInitializers) {
               if (sci instanceof GeneratedSchema gs && !discoveredSchemas.contains(gs)) {
                  discoveredSchemas.add(gs);
               }
               builder.addContextInitializer(sci);
               registeredClasses.add(sci.getClass().getName());
            }
         }

         // Register classpath-discovered schemas with the builder
         // Skipping those already registered as beans
         for (GeneratedSchema schema : discoveredSchemas) {
            if (!registeredClasses.contains(schema.getClass().getName())) {
               builder.addContextInitializer(schema);
            }
         }
      }

      cacheCustomizers.forEach(c -> c.customize(builder));
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());

      if (infinispanProperties.isUseSchemaRegistration()) {
         SchemaRegistration.uploadSchemas(remoteCacheManager, discoveredSchemas);
      }

      return remoteCacheManager;
   }

   public static class ConditionalOnConfiguration extends AnyNestedCondition {
      public ConditionalOnConfiguration() {
         super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
      }

      @Conditional(ConditionalOnConfigurationResources.class)
      static class OnConfigurationResources {
      }

      @ConditionalOnBean(InfinispanRemoteConfigurer.class)
      static class OnRemoteConfigurer {
      }

      @ConditionalOnBean(org.infinispan.client.hotrod.configuration.Configuration.class)
      static class OnConfiguration {
      }
   }

   public static class ConditionalOnCacheType implements Condition {
      @Override
      public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata atm) {
         String cacheType = ctx.getEnvironment().getProperty("spring.cache.type");
         return cacheType == null || CacheType.INFINISPAN.name().equalsIgnoreCase(cacheType);
      }
   }

   public static class ConditionalOnConfigurationResources implements Condition {
      @Override
      public boolean matches(ConditionContext ctx, AnnotatedTypeMetadata atm) {
         return hasHotRodClientPropertiesFile(ctx) || hasServersProperty(ctx);
      }

      private boolean hasServersProperty(ConditionContext conditionContext) {
         return conditionContext.getEnvironment().getProperty("infinispan.remote.server-list") != null;
      }

      private boolean hasHotRodClientPropertiesFile(ConditionContext conditionContext) {
         String hotRodPropertiesPath = conditionContext.getEnvironment().getProperty("infinispan.remote.client-properties");
         if (hotRodPropertiesPath == null) {
            hotRodPropertiesPath = InfinispanRemoteConfigurationProperties.DEFAULT_CLIENT_PROPERTIES;
         }

         return conditionContext.getResourceLoader().getResource(hotRodPropertiesPath).exists();
      }
   }

}
