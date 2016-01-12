package org.infinispan.configuration.cache;

import static java.util.Arrays.asList;
import static org.infinispan.configuration.cache.Configuration.INLINE_INTERCEPTORS;
import static org.infinispan.configuration.cache.Configuration.SIMPLE_CACHE;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ConfigurationBuilder implements ConfigurationChildBuilder {
   private static final Log log = LogFactory.getLog(ConfigurationBuilder.class, Log.class);

   private final ClusteringConfigurationBuilder clustering;
   private final CustomInterceptorsConfigurationBuilder customInterceptors;
   private final DataContainerConfigurationBuilder dataContainer;
   private final DeadlockDetectionConfigurationBuilder deadlockDetection;
   private final EvictionConfigurationBuilder eviction;
   private final ExpirationConfigurationBuilder expiration;
   private final IndexingConfigurationBuilder indexing;
   private final InvocationBatchingConfigurationBuilder invocationBatching;
   private final JMXStatisticsConfigurationBuilder jmxStatistics;
   private final PersistenceConfigurationBuilder persistence;
   private final LockingConfigurationBuilder locking;
   private final SecurityConfigurationBuilder security;
   private final StoreAsBinaryConfigurationBuilder storeAsBinary;
   private final TransactionConfigurationBuilder transaction;
   private final VersioningConfigurationBuilder versioning;
   private final UnsafeConfigurationBuilder unsafe;
   private final List<Builder<?>> modules = new ArrayList<Builder<?>>();
   private final SitesConfigurationBuilder sites;
   private final CompatibilityModeConfigurationBuilder compatibility;
   private final AttributeSet attributes;

   private boolean template = false;

   public ConfigurationBuilder() {
      this.attributes = Configuration.attributeDefinitionSet();
      this.clustering = new ClusteringConfigurationBuilder(this);
      this.customInterceptors = new CustomInterceptorsConfigurationBuilder(this);
      this.dataContainer = new DataContainerConfigurationBuilder(this);
      this.deadlockDetection = new DeadlockDetectionConfigurationBuilder(this);
      this.eviction = new EvictionConfigurationBuilder(this);
      this.expiration = new ExpirationConfigurationBuilder(this);
      this.indexing = new IndexingConfigurationBuilder(this);
      this.invocationBatching = new InvocationBatchingConfigurationBuilder(this);
      this.jmxStatistics = new JMXStatisticsConfigurationBuilder(this);
      this.persistence = new PersistenceConfigurationBuilder(this);
      this.locking = new LockingConfigurationBuilder(this);
      this.security = new SecurityConfigurationBuilder(this);
      this.storeAsBinary = new StoreAsBinaryConfigurationBuilder(this);
      this.transaction = new TransactionConfigurationBuilder(this);
      this.versioning = new VersioningConfigurationBuilder(this);
      this.unsafe = new UnsafeConfigurationBuilder(this);
      this.sites = new SitesConfigurationBuilder(this);
      this.compatibility = new CompatibilityModeConfigurationBuilder(this);
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
   public ConfigurationBuilder inlineInterceptors(boolean inlineInterceptors) {
      // TODO: check that JVM properties are set properly so that interceptor stack
      // can be really inlined; recommended values are
      // -XX:FreqInlineSize=450 -XX:MaxInlineSize=100 -XX:MaxInlineLevel=50
      attributes.attribute(INLINE_INTERCEPTORS).set(inlineInterceptors);
      return this;
   }

   @Override
   public boolean inlineInterceptors() { return attributes.attribute(INLINE_INTERCEPTORS).get(); }

   @Override
   public ClusteringConfigurationBuilder clustering() {
      return clustering;
   }

   @Override
   public CustomInterceptorsConfigurationBuilder customInterceptors() {
      return customInterceptors;
   }

   @Override
   public DataContainerConfigurationBuilder dataContainer() {
      return dataContainer;
   }

   @Override
   public DeadlockDetectionConfigurationBuilder deadlockDetection() {
      return deadlockDetection;
   }

   @Override
   public EvictionConfigurationBuilder eviction() {
      return eviction;
   }

   @Override
   public ExpirationConfigurationBuilder expiration() {
      return expiration;
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
   public JMXStatisticsConfigurationBuilder jmxStatistics() {
      return jmxStatistics;
   }

   @Override
   public StoreAsBinaryConfigurationBuilder storeAsBinary() {
      return storeAsBinary;
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
   public VersioningConfigurationBuilder versioning() {
      return versioning;
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
   public CompatibilityModeConfigurationBuilder compatibility() {
      return compatibility;
   }

   public List<Builder<?>> modules() {
      return modules;
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

   @SuppressWarnings("unchecked")
   public void validate() {
      if (attributes.attribute(SIMPLE_CACHE).get()) {
         validateSimpleCacheConfiguration();
      }
      for (Builder<?> validatable:
            asList(clustering, customInterceptors, dataContainer, deadlockDetection, eviction, expiration, indexing,
                   invocationBatching, jmxStatistics, persistence, locking, storeAsBinary, transaction,
                   versioning, unsafe, sites, compatibility)) {
         validatable.validate();
      }
      for (Builder<?> m : modules) {
         m.validate();
      }
   }

   private void validateSimpleCacheConfiguration() {
      if (clustering().cacheMode().isClustered()
            || (transaction.transactionMode() != null && transaction.transactionMode().isTransactional())
            || !customInterceptors.create().interceptors().isEmpty()
            || !persistence.stores().isEmpty()
            || invocationBatching.isEnabled()
            || indexing.enabled()
            || storeAsBinary.create().enabled()
            || compatibility.create().enabled()) {
         throw log.notSupportedInSimpleCache();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      for (ConfigurationChildBuilder validatable:
            asList(clustering, customInterceptors, dataContainer, deadlockDetection, eviction, expiration, indexing,
                   invocationBatching, jmxStatistics, persistence, locking, storeAsBinary, transaction,
                   versioning, unsafe, sites, compatibility)) {
         validatable.validate(globalConfig);
      }
      // Modules cannot be checked with GlobalConfiguration
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
      List<Object> modulesConfig = new LinkedList<Object>();
      for (Builder<?> module : modules)
         modulesConfig.add(module.create());
      return new Configuration(template, attributes.protect(), clustering.create(), customInterceptors.create(),
               dataContainer.create(), deadlockDetection.create(), eviction.create(),
               expiration.create(), indexing.create(), invocationBatching.create(),
               jmxStatistics.create(), persistence.create(), locking.create(), security.create(),
               storeAsBinary.create(), transaction.create(), unsafe.create(), versioning.create(), sites.create(),
               compatibility.create(),
               modulesConfig);
   }

   public ConfigurationBuilder read(Configuration template) {
      this.attributes.read(template.attributes());
      this.clustering.read(template.clustering());
      this.customInterceptors.read(template.customInterceptors());
      this.dataContainer.read(template.dataContainer());
      this.deadlockDetection.read(template.deadlockDetection());
      this.eviction.read(template.eviction());
      this.expiration.read(template.expiration());
      this.indexing.read(template.indexing());
      this.invocationBatching.read(template.invocationBatching());
      this.jmxStatistics.read(template.jmxStatistics());
      this.persistence.read(template.persistence());
      this.locking.read(template.locking());
      this.security.read(template.security());
      this.storeAsBinary.read(template.storeAsBinary());
      this.transaction.read(template.transaction());
      this.unsafe.read(template.unsafe());
      this.sites.read(template.sites());
      this.versioning.read(template.versioning());
      this.compatibility.read(template.compatibility());

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
            ", dataContainer=" + dataContainer +
            ", deadlockDetection=" + deadlockDetection +
            ", eviction=" + eviction +
            ", expiration=" + expiration +
            ", indexing=" + indexing +
            ", invocationBatching=" + invocationBatching +
            ", jmxStatistics=" + jmxStatistics +
            ", persistence=" + persistence +
            ", locking=" + locking +
            ", modules=" + modules +
            ", security=" + security +
            ", storeAsBinary=" + storeAsBinary +
            ", transaction=" + transaction +
            ", versioning=" + versioning +
            ", unsafe=" + unsafe +
            ", sites=" + sites +
            ", compatibility=" + compatibility +
            '}';
   }

}
