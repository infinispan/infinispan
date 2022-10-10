package org.infinispan.configuration.cache;

import static java.util.Arrays.asList;
import static org.infinispan.configuration.cache.Configuration.SIMPLE_CACHE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

public class ConfigurationBuilder implements ConfigurationChildBuilder {
   private final ClusteringConfigurationBuilder clustering;
   private final CustomInterceptorsConfigurationBuilder customInterceptors;
   private final EncodingConfigurationBuilder encoding;
   private final ExpirationConfigurationBuilder expiration;
   private final QueryConfigurationBuilder query;
   private final IndexingConfigurationBuilder indexing;
   private final InvocationBatchingConfigurationBuilder invocationBatching;
   private final StatisticsConfigurationBuilder statistics;
   private final PersistenceConfigurationBuilder persistence;
   private final LockingConfigurationBuilder locking;
   private final SecurityConfigurationBuilder security;
   private final TransactionConfigurationBuilder transaction;
   private final UnsafeConfigurationBuilder unsafe;
   private final List<Builder<?>> modules = new ArrayList<>();
   private final SitesConfigurationBuilder sites;
   private final MemoryConfigurationBuilder memory;
   private final AttributeSet attributes;

   private boolean template = false;

   public ConfigurationBuilder() {
      this.attributes = Configuration.attributeDefinitionSet();
      this.clustering = new ClusteringConfigurationBuilder(this);
      this.customInterceptors = new CustomInterceptorsConfigurationBuilder(this);
      this.encoding = new EncodingConfigurationBuilder(this);
      this.expiration = new ExpirationConfigurationBuilder(this);
      this.query = new QueryConfigurationBuilder(this);
      this.indexing = new IndexingConfigurationBuilder(this);
      this.invocationBatching = new InvocationBatchingConfigurationBuilder(this);
      this.statistics = new StatisticsConfigurationBuilder(this);
      this.persistence = new PersistenceConfigurationBuilder(this);
      this.locking = new LockingConfigurationBuilder(this);
      this.security = new SecurityConfigurationBuilder(this);
      this.transaction = new TransactionConfigurationBuilder(this);
      this.unsafe = new UnsafeConfigurationBuilder(this);
      this.sites = new SitesConfigurationBuilder(this);
      this.memory = new MemoryConfigurationBuilder(this);
   }

   @Override
   public ConfigurationBuilder simpleCache(boolean simpleCache) {
      attributes.attribute(SIMPLE_CACHE).set(simpleCache);
      return this;
   }

   @Override
   public boolean simpleCache() {
      return attributes.attribute(SIMPLE_CACHE).get();
   }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return clustering;
   }

   /**
    * @deprecated Since 10.0, custom interceptors support will be removed and only modules will be able to define interceptors
    */
   @Deprecated
   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return customInterceptors;
   }

   @Override
   public EncodingConfigurationBuilder encoding() {
      return encoding;
   }

   @Override
   public ExpirationConfigurationBuilder expiration() {
      return expiration;
   }

   @Override
   public QueryConfigurationBuilder query() {
      return query;
   }

   @Override
   public IndexingConfigurationBuilder indexing() {
      return indexing;
   }

   @Override
   public InvocationBatchingConfigurationBuilder invocationBatching() {
      return invocationBatching;
   }

   @Override
   public StatisticsConfigurationBuilder statistics() {
      return statistics;
   }

   @Override
   public PersistenceConfigurationBuilder persistence() {
      return persistence;
   }

   @Override
   public LockingConfigurationBuilder locking() {
      return locking;
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public TransactionConfigurationBuilder transaction() {
      return transaction;
   }

   @Override
   public UnsafeConfigurationBuilder unsafe() {
      return unsafe;
   }

   @Override
   public SitesConfigurationBuilder sites() {
      return sites;
   }

   @Override
   public MemoryConfigurationBuilder memory() { return memory; }

   public List<Builder<?>> modules() {
      return Collections.unmodifiableList(modules);
   }

   public ConfigurationBuilder clearModules() {
      modules.clear();
      return this;
   }

   public <T extends Builder<?>> T addModule(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(ConfigurationBuilder.class);
         T builder = constructor.newInstance(this);
         this.modules.add(builder);
         return builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate module configuration builder '" + klass.getName() + "'", e);
      }
   }

   @Override
   public ConfigurationBuilder template(boolean template) {
      this.template = template;
      return this;
   }

   public boolean template() {
      return template;
   }

   public void validate() {
      if (attributes.attribute(SIMPLE_CACHE).get()) {
         validateSimpleCacheConfiguration();
      }
      List<RuntimeException> validationExceptions = new ArrayList<>();
      for (Builder<?> validatable:
            asList(clustering, customInterceptors, expiration, indexing, encoding,
                   invocationBatching, statistics, persistence, locking, transaction,
                   unsafe, sites, memory)) {
         try {
            validatable.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      }
      for (Builder<?> m : modules) {
         try {
            m.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      }
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> { throw e; });
   }

   private void validateSimpleCacheConfiguration() {
      if (clustering().cacheMode().isClustered()
            || (transaction.transactionMode() != null && transaction.transactionMode().isTransactional())
            || !customInterceptors.create().interceptors().isEmpty()
            || !persistence.stores().isEmpty()
            || invocationBatching.isEnabled()
            || indexing.enabled()
            || memory.create().storage() == StorageType.BINARY) {
         throw CONFIG.notSupportedInSimpleCache();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      List<RuntimeException> validationExceptions = new ArrayList<>();
      for (ConfigurationChildBuilder validatable:
            asList(clustering, customInterceptors, expiration, indexing,
                   invocationBatching, statistics, persistence, locking, transaction,
                   unsafe, sites, security, memory)) {
         try {
            validatable.validate(globalConfig);
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      }
      // Modules cannot be checked with GlobalConfiguration
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> { throw e; });
   }

   @Override
   public Configuration build() {
      return build(true);
   }

   public Configuration build(GlobalConfiguration globalConfiguration) {
      validate(globalConfiguration);
      return build(true);
   }

   public Configuration build(boolean validate) {
      if (validate) {
         validate();
      }
      List<Object> modulesConfig = new LinkedList<>();
      for (Builder<?> module : modules)
         modulesConfig.add(module.create());
      return new Configuration(template, attributes.protect(), clustering.create(), customInterceptors.create(),
                               expiration.create(), encoding.create(), query.create(), indexing.create(), invocationBatching.create(),
                               statistics.create(), persistence.create(), locking.create(), security.create(),
                               transaction.create(), unsafe.create(), sites.create(), memory.create(), modulesConfig);
   }

   public ConfigurationBuilder read(Configuration template) {
      this.attributes.read(template.attributes());
      this.clustering.read(template.clustering());
      this.customInterceptors.read(template.customInterceptors());
      this.expiration.read(template.expiration());
      this.query.read(template.query());
      this.indexing.read(template.indexing());
      this.invocationBatching.read(template.invocationBatching());
      this.statistics.read(template.statistics());
      this.persistence.read(template.persistence());
      this.locking.read(template.locking());
      this.security.read(template.security());
      this.transaction.read(template.transaction());
      this.unsafe.read(template.unsafe());
      this.sites.read(template.sites());
      this.memory.read(template.memory());
      this.encoding.read(template.encoding());
      this.template = template.isTemplate();

      for (Object c : template.modules().values()) {
         Builder<Object> builder = this.addModule(ConfigurationUtils.builderFor(c));
         builder.read(c);
      }

      return this;
   }

   @Override
   public String toString() {
      return "ConfigurationBuilder{" +
            "clustering=" + clustering +
            ", customInterceptors=" + customInterceptors +
            ", expiration=" + expiration +
            ", query=" + query +
            ", indexing=" + indexing +
            ", invocationBatching=" + invocationBatching +
            ", statistics=" + statistics +
            ", persistence=" + persistence +
            ", locking=" + locking +
            ", modules=" + modules +
            ", security=" + security +
            ", transaction=" + transaction +
            ", unsafe=" + unsafe +
            ", sites=" + sites +
            '}';
   }

}
