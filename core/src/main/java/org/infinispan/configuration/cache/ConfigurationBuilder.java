package org.infinispan.configuration.cache;

import static java.util.Arrays.asList;
import static org.infinispan.configuration.cache.Configuration.ALIASES;
import static org.infinispan.configuration.cache.Configuration.CONFIGURATION;
import static org.infinispan.configuration.cache.Configuration.SIMPLE_CACHE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

public class ConfigurationBuilder implements ConfigurationChildBuilder {
   private final ClusteringConfigurationBuilder clustering;
   private final EncodingConfigurationBuilder encoding;
   private final ExpirationConfigurationBuilder expiration;
   private final QueryConfigurationBuilder query;
   private final IndexingConfigurationBuilder indexing;
   private final TracingConfigurationBuilder tracing;
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
      this.encoding = new EncodingConfigurationBuilder(this);
      this.expiration = new ExpirationConfigurationBuilder(this);
      this.query = new QueryConfigurationBuilder(this);
      this.indexing = new IndexingConfigurationBuilder(this);
      this.tracing = new TracingConfigurationBuilder(this);
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
   public ConfigurationBuilder aliases(String... aliases) {
      Set<String> existing = attributes.attribute(ALIASES).get();
      Collections.addAll(existing, aliases);
      attributes.attribute(ALIASES).set(existing);
      return this;
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
   public TracingConfigurationBuilder tracing() {
      return tracing;
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
   public MemoryConfigurationBuilder memory() {
      return memory;
   }

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

   public ConfigurationBuilder configuration(String baseConfigurationName) {
      attributes.attribute(CONFIGURATION).set(baseConfigurationName);
      return this;
   }

   public String configuration() {
      return attributes.attribute(CONFIGURATION).get();
   }

   public void validate() {
      if (attributes.attribute(SIMPLE_CACHE).get()) {
         validateSimpleCacheConfiguration();
      }
      List<RuntimeException> validationExceptions = new ArrayList<>();
      for (Builder<?> validatable : asList(
            clustering,
            encoding,
            expiration,
            indexing,
            invocationBatching,
            locking,
            memory,
            persistence,
            query,
            sites,
            statistics,
            tracing,
            transaction,
            unsafe
      )) {
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
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> {
         throw e;
      });
   }

   private void validateSimpleCacheConfiguration() {
      if (clustering().cacheMode().isClustered()
            || (transaction.transactionMode() != null && transaction.transactionMode().isTransactional())
            || !persistence.stores().isEmpty()
            || invocationBatching.isEnabled()
            || indexing.enabled()) {
         throw CONFIG.notSupportedInSimpleCache();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      List<RuntimeException> validationExceptions = new ArrayList<>();
      for (ConfigurationChildBuilder validatable : asList(
            clustering,
            encoding,
            expiration,
            indexing,
            invocationBatching,
            locking,
            memory,
            persistence,
            query,
            security,
            sites,
            statistics,
            tracing,
            transaction,
            unsafe
      )) {
         try {
            validatable.validate(globalConfig);
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      }
      // Modules cannot be checked with GlobalConfiguration
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> {
         throw e;
      });
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
      return new Configuration(template, attributes.protect(),
            clustering.create(),
            encoding.create(),
            expiration.create(),
            indexing.create(),
            invocationBatching.create(),
            locking.create(),
            memory.create(),
            persistence.create(),
            query.create(),
            security.create(),
            sites.create(),
            statistics.create(),
            tracing.create(),
            transaction.create(),
            unsafe.create(),
            modulesConfig
      );
   }

   public ConfigurationBuilder read(Configuration template) {
      return read(template, Combine.DEFAULT);
   }

   public ConfigurationBuilder read(Configuration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.clustering.read(template.clustering(), combine);
      this.expiration.read(template.expiration(), combine);
      this.query.read(template.query(), combine);
      this.indexing.read(template.indexing(), combine);
      this.tracing.read(template.tracing());
      this.invocationBatching.read(template.invocationBatching(), combine);
      this.statistics.read(template.statistics(), combine);
      this.persistence.read(template.persistence(), combine);
      this.locking.read(template.locking(), combine);
      this.security.read(template.security(), combine);
      this.transaction.read(template.transaction(), combine);
      this.unsafe.read(template.unsafe(), combine);
      this.sites.read(template.sites(), combine);
      this.memory.read(template.memory(), combine);
      this.encoding.read(template.encoding(), combine);
      this.template = template.isTemplate();

      for (Object c : template.modules().values()) {
         Builder<Object> builder = this.addModule(ConfigurationUtils.builderFor(c));
         builder.read(c, combine);
      }

      return this;
   }

   @Override
   public String toString() {
      return "ConfigurationBuilder{" +
            attributes +
            ", clustering=" + clustering +
            ", expiration=" + expiration +
            ", query=" + query +
            ", indexing=" + indexing +
            ", tracing=" + tracing +
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
