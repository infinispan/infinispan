package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.CacheContainerConfiguration.ASYNC_EXECUTOR;
import static org.infinispan.configuration.global.CacheContainerConfiguration.DEFAULT_CACHE;
import static org.infinispan.configuration.global.CacheContainerConfiguration.EXPIRATION_EXECUTOR;
import static org.infinispan.configuration.global.CacheContainerConfiguration.LISTENER_EXECUTOR;
import static org.infinispan.configuration.global.CacheContainerConfiguration.NAME;
import static org.infinispan.configuration.global.CacheContainerConfiguration.PERSISTENCE_EXECUTOR;
import static org.infinispan.configuration.global.CacheContainerConfiguration.STATE_TRANSFER_EXECUTOR;
import static org.infinispan.configuration.global.CacheContainerConfiguration.STATISTICS;
import static org.infinispan.configuration.global.CacheContainerConfiguration.ZERO_CAPACITY_NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class CacheContainerConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<CacheContainerConfiguration> {

   private final AttributeSet attributes;
   private final GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics;
   private final GlobalStateConfigurationBuilder globalState;
   private final TransportConfigurationBuilder transport;
   private final GlobalSecurityConfigurationBuilder security;
   private final SerializationConfigurationBuilder serialization;
   private final ShutdownConfigurationBuilder shutdown;
   private final ThreadsConfigurationBuilder threads;

   CacheContainerConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.attributes = CacheContainerConfiguration.attributeDefinitionSet();
      this.globalJmxStatistics = new GlobalJmxStatisticsConfigurationBuilder(globalConfig);
      this.globalState = new GlobalStateConfigurationBuilder(globalConfig);
      this.threads = new ThreadsConfigurationBuilder(globalConfig);
      this.transport = new TransportConfigurationBuilder(globalConfig, threads);
      this.security = new GlobalSecurityConfigurationBuilder(globalConfig);
      this.serialization = new SerializationConfigurationBuilder(globalConfig);
      this.shutdown = new ShutdownConfigurationBuilder(globalConfig);
   }

   public CacheContainerConfigurationBuilder clusteredDefault() {
      transport().
            defaultTransport()
            .clearProperties();
      return this;
   }

   public CacheContainerConfigurationBuilder nonClusteredDefault() {
      transport()
            .transport(null)
            .clearProperties();
      return this;
   }

   public static GlobalConfigurationBuilder defaultClusteredBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.transport().defaultTransport();
      return builder;
   }

   public String defaultCacheName() {
      return attributes.attribute(DEFAULT_CACHE).get();
   }

   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      return globalJmxStatistics;
   }

   @Override
   public GlobalStateConfigurationBuilder globalState() {
      return globalState;
   }

   @Override
   public TransportConfigurationBuilder transport() {
      return transport;
   }

   public ThreadsConfigurationBuilder threads() {
      return threads;
   }

   @Override
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return threads.asyncThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder expirationThreadPool() {
      return threads.expirationThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder persistenceThreadPool() {
      return threads.persistenceThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder stateTransferThreadPool() {
      return threads.stateTransferThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder listenerThreadPool() {
      return threads.listenerThreadPool();
   }

   @Override
   public GlobalSecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return serialization;
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return shutdown;
   }

   public CacheContainerConfigurationBuilder defaultCache(String defaultCacheName) {
      attributes.attribute(DEFAULT_CACHE).set(defaultCacheName);
      return this;
   }

   public CacheContainerConfigurationBuilder name(String cacheManagerName) {
      attributes.attribute(NAME).set(cacheManagerName);
      return this;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public CacheContainerConfigurationBuilder statistics(Boolean statistics) {
      attributes.attribute(STATISTICS).set(statistics);
      return this;
   }

   public boolean statistics() {
      return attributes.attribute(STATISTICS).get();
   }

   CacheContainerConfigurationBuilder zeroCapacityNode(boolean zeroCapacityNode) {
      attributes.attribute(ZERO_CAPACITY_NODE).set(zeroCapacityNode);
      return this;
   }

   public CacheContainerConfigurationBuilder asyncExecutor(String name) {
      attributes.attribute(ASYNC_EXECUTOR).set(name);
      return this;
   }

   CacheContainerConfigurationBuilder listenerExecutor(String name) {
      attributes.attribute(LISTENER_EXECUTOR).set(name);
      return this;
   }

   CacheContainerConfigurationBuilder expirationExecutor(String name) {
      attributes.attribute(EXPIRATION_EXECUTOR).set(name);
      return this;
   }

   public CacheContainerConfigurationBuilder persistenceExecutor(String name) {
      attributes.attribute(PERSISTENCE_EXECUTOR).set(name);
      return this;
   }

   public CacheContainerConfigurationBuilder stateTransferExecutor(String name) {
      attributes.attribute(STATE_TRANSFER_EXECUTOR).set(name);
      return this;
   }

   public void validate() {
      List<RuntimeException> validationExceptions = new ArrayList<>();
      Arrays.asList(
            globalJmxStatistics,
            globalState,
            transport,
            security,
            serialization,
            shutdown,
            threads
      ).forEach(c -> {
         try {
            c.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      });
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> {
         throw e;
      });
   }

   @Override
   public CacheContainerConfiguration create() {
      Attribute<String> attribute = attributes.attribute(NAME);
      if (!attribute.isModified()) {
         name(attribute.getAttributeDefinition().getDefaultValue());
      }
      return new CacheContainerConfiguration(
            attributes.protect(),
            threads.create(),
            globalJmxStatistics.create(),
            transport.create(),
            security.create(),
            serialization.create(),
            globalState.create(),
            shutdown.create(),
            getGlobalConfig().getFeatures()
            );
   }

   @Override
   public Builder<?> read(CacheContainerConfiguration template) {
      attributes.read(template.attributes());
      this.globalState.read(template.globalState());
      this.globalJmxStatistics.read(template.globalJmxStatistics());
      this.transport.read(template.transport());
      this.security.read(template.security());
      this.serialization.read(template.serialization());
      this.shutdown.read(template.shutdown());
      this.threads.read(template.threads());
      return this;
   }
}
