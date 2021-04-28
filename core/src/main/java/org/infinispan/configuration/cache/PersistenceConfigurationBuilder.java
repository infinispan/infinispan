package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.PersistenceConfiguration.AVAILABILITY_INTERVAL;
import static org.infinispan.configuration.cache.PersistenceConfiguration.CONNECTION_ATTEMPTS;
import static org.infinispan.configuration.cache.PersistenceConfiguration.CONNECTION_INTERVAL;
import static org.infinispan.configuration.cache.PersistenceConfiguration.PASSIVATION;
import static org.infinispan.configuration.parsing.Element.CLUSTER_LOADER;
import static org.infinispan.configuration.parsing.Element.FILE_STORE;
import static org.infinispan.configuration.parsing.Element.SINGLE_FILE_STORE;
import static org.infinispan.configuration.parsing.Element.STORE;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;

/**
 * Configuration for cache stores.
 *
 */
public class PersistenceConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<PersistenceConfiguration>, ConfigurationBuilderInfo {
   private final List<StoreConfigurationBuilder<?, ?>> stores = new ArrayList<>(2);
   private final AttributeSet attributes;

   private static final Set<Class<? extends StoreConfigurationBuilder<?, ?>>> AVAILABLE_BUILDERS =
         Configurations.lookupPersistenceBuilders();

   private static List<Class<? extends StoreConfigurationBuilder<?, ?>>> subElements = new ArrayList<>();

   protected PersistenceConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = PersistenceConfiguration.attributeDefinitionSet();
   }


   public StoreConfigurationBuilder getBuilderFromName(String name) {
      Optional<Class<? extends StoreConfigurationBuilder<?, ?>>> first = AVAILABLE_BUILDERS.stream().filter(klass -> {
         StoreConfigurationBuilder<?, ?> builderFromClass = getBuilderFromClass(klass);
         return builderFromClass.getElementDefinition().supports(name);
      }).findFirst();

      return getBuilderFromClass(first.get());
   }


   public PersistenceConfigurationBuilder passivation(boolean b) {
      attributes.attribute(PASSIVATION).set(b);
      return this;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return PersistenceConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ConfigurationBuilderInfo getNewBuilderInfo(String name) {
      return getBuilderInfo(name, null);
   }

   @Override
   public ConfigurationBuilderInfo getBuilderInfo(String name, String qualifier) {
      if (name.equals(FILE_STORE.getLocalName())) {
         return addSoftIndexFileStore();
      }
      if (name.equals(SINGLE_FILE_STORE.getLocalName())) {
         return addSingleFileStore();
      }
      if (name.equals(CLUSTER_LOADER.getLocalName())) {
         return addClusterLoader();
      }
      if (name.equals(STORE.getLocalName())) {
         Object store = Util.getInstance(qualifier, Thread.currentThread().getContextClassLoader());
         ConfiguredBy annotation = store.getClass().getAnnotation(ConfiguredBy.class);
         Class<? extends StoreConfigurationBuilder> builderClass = null;
         if (annotation != null) {
            Class<?> configuredBy = annotation.value();
            BuiltBy builtBy = configuredBy.getAnnotation(BuiltBy.class);
            builderClass = builtBy.value().asSubclass(StoreConfigurationBuilder.class);
         }
         StoreConfigurationBuilder configBuilder;
         if (builderClass == null) {
            configBuilder = addStore(CustomStoreConfigurationBuilder.class).customStoreClass(store.getClass());
         } else {
            configBuilder = addStore(builderClass);
         }

         return configBuilder;

      }
      StoreConfigurationBuilder builder = getBuilderFromName(name);
      this.addStore(builder);
      return builder;
   }

   /**
    * @param interval The time, in milliseconds, between availability checks to determine if the PersistenceManager
    *           is available. In other words, this interval sets how often stores/loaders are polled via their
    *           `org.infinispan.persistence.spi.CacheWriter#isAvailable` or
    *           `org.infinispan.persistence.spi.CacheLoader#isAvailable` implementation. If a single store/loader is
    *           not available, an exception is thrown during cache operations.
    */
   public PersistenceConfigurationBuilder availabilityInterval(int interval) {
      attributes.attribute(AVAILABILITY_INTERVAL).set(interval);
      return this;
   }

   /**
    * @param attempts The maximum number of unsuccessful attempts to start each of the configured CacheWriter/CacheLoader
    *                 before an exception is thrown and the cache fails to start.
    */
   public PersistenceConfigurationBuilder connectionAttempts(int attempts) {
      attributes.attribute(CONNECTION_ATTEMPTS).set(attempts);
      return this;
   }

   /**
    * @param interval The time, in milliseconds, to wait between subsequent connection attempts on startup. A negative
    *                 or zero value means no wait between connection attempts.
    */
   public PersistenceConfigurationBuilder connectionInterval(int interval) {
      attributes.attribute(CONNECTION_INTERVAL).set(interval);
      return this;
   }

   /**
    * If true, data is written to the cache store only when it is evicted from memory, which is known as 'passivation'.
    * When the data is requested again it is activated, which returns the data to memory and removes it from the
    * persistent store. As a result, you can overflow data to disk, similarly to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory. Write operations to the cache result in
    * writes to the cache store, which is equivalent to a 'write-through' configuration.
    */
   boolean passivation() {
      return attributes.attribute(PASSIVATION).get();
   }

   /**
    * Adds a cache loader that uses the specified builder class to build its configuration.
    */
   public <T extends StoreConfigurationBuilder<?, ?>> T addStore(Class<T> klass) {
      T builder = getBuilderFromClass(klass);
      this.stores.add(builder);
      return builder;
   }

   private <T extends StoreConfigurationBuilder<?, ?>> T getBuilderFromClass(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(PersistenceConfigurationBuilder.class);
         return constructor.newInstance(this);
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate loader configuration builder '" + klass.getName()
               + "'", e);
      }
   }

   /**
    * Adds a cache loader that uses the specified builder instance to build its configuration.
    *
    * @param builder is an instance of {@link StoreConfigurationBuilder}.
    */
   public StoreConfigurationBuilder<?, ?> addStore(StoreConfigurationBuilder<?, ?> builder) {
      this.stores.add(builder);
      return builder;
   }

   /**
    * Adds a cluster cache loader.
    * @deprecated since 11.0. To be removed in 14.0 ISPN-11864 with no direct replacement.
    */
   @Deprecated
   public ClusterLoaderConfigurationBuilder addClusterLoader() {
      ClusterLoaderConfigurationBuilder builder = new ClusterLoaderConfigurationBuilder(this);
      this.stores.add(builder);
      return builder;
   }

   /**
    * Adds a single file cache store.
    * @deprecated since 13.0. To be removed in 14.0 has been replaced by {@link #addSoftIndexFileStore()}
    */
   @Deprecated
   public SingleFileStoreConfigurationBuilder addSingleFileStore() {
      SingleFileStoreConfigurationBuilder builder = new SingleFileStoreConfigurationBuilder(this);
      this.stores.add(builder);
      return builder;
   }

   /**
    * Adds a soft index file cache store.
    * @return the configuration for a soft index file store
    */
   public SoftIndexFileStoreConfigurationBuilder addSoftIndexFileStore() {
      SoftIndexFileStoreConfigurationBuilder builder = new SoftIndexFileStoreConfigurationBuilder(this);
      this.stores.add(builder);
      return builder;
   }

   /**
    * Removes any configured stores from this builder.
    */
   public PersistenceConfigurationBuilder clearStores() {
      this.stores.clear();
      return this;
   }

   @Override
   public void validate() {
      boolean isLocalCache = builder.clustering().create().cacheMode().equals(CacheMode.LOCAL);
      int numFetchPersistentState = 0;
      int numPreload = 0;
      for (StoreConfigurationBuilder<?, ?> b : stores) {
         b.validate();
         StoreConfiguration storeConfiguration = b.create();
         if (storeConfiguration.shared()) {
            if (b.persistence().passivation()) {
               throw CONFIG.passivationStoreCannotBeShared(storeConfiguration.getClass().getSimpleName());
            }
            if (storeConfiguration.purgeOnStartup()) {
               throw CONFIG.sharedStoreShouldNotBePurged(storeConfiguration.getClass().getSimpleName());
            }
         } else if (storeConfiguration.transactional() && !isLocalCache) {
            throw CONFIG.clusteredTransactionalStoreMustBeShared(storeConfiguration.getClass().getSimpleName());
         }
         if (storeConfiguration.async().enabled() && storeConfiguration.transactional()) {
            throw CONFIG.transactionalStoreCannotBeAsync(storeConfiguration.getClass().getSimpleName());
         }
         if (storeConfiguration.fetchPersistentState()) {
            numFetchPersistentState++;
         }
         if (storeConfiguration.preload()) {
            numPreload++;
         }
      }
      if (numFetchPersistentState > 1) {
         throw CONFIG.onlyOneFetchPersistentStoreAllowed();
      }
      if (numPreload > 1) {
         throw CONFIG.onlyOnePreloadStoreAllowed();
      }

      // If a store is present, the reaper expiration thread must be enabled.
      if (!stores.isEmpty()) {
         boolean reaperEnabled = builder.expiration().reaperEnabled();
         long wakeupInterval = builder.expiration().wakeupInterval();
         if (!reaperEnabled || wakeupInterval < 0) {
            builder.expiration().enableReaper();
            if (wakeupInterval < 0) {
               CONFIG.debug("Store present and expiration reaper wakeup was less than 0 - explicitly enabling and setting " +
                       "wakeup interval to 1 minute.");
               builder.expiration().wakeUpInterval(1, TimeUnit.MINUTES);
            } else {
               CONFIG.debug("Store present however expiration reaper was not enabled - explicitly enabling.");
            }
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      for (StoreConfigurationBuilder<?, ?> b : stores) {
         b.validate(globalConfig);
      }
   }

   @Override
   public PersistenceConfiguration create() {
      List<StoreConfiguration> stores = new ArrayList<StoreConfiguration>(this.stores.size());
      for (StoreConfigurationBuilder<?, ?> loader : this.stores)
         stores.add(loader.create());
      return new PersistenceConfiguration(attributes.protect(), stores);
   }

   @SuppressWarnings("unchecked")
   @Override
   public PersistenceConfigurationBuilder read(PersistenceConfiguration template) {
      this.attributes.read(template.attributes());
      clearStores();
      for (StoreConfiguration c : template.stores()) {
         Class<? extends StoreConfigurationBuilder<?, ?>> builderClass = getBuilderClass(c);
         StoreConfigurationBuilder builder =  this.getBuilderFromClass(builderClass);
         stores.add((StoreConfigurationBuilder) builder.read(c));
      }

      return this;
   }

   private Class<? extends StoreConfigurationBuilder<?, ?>> getBuilderClass(StoreConfiguration c) {
      Class<? extends StoreConfigurationBuilder<?, ?>> builderClass = (Class<? extends StoreConfigurationBuilder<?, ?>>) ConfigurationUtils.builderForNonStrict(c);
      if (builderClass == null) {
         builderClass = CustomStoreConfigurationBuilder.class;
      }
      return builderClass;
   }

   public List<StoreConfigurationBuilder<?, ?>> stores() {
      return stores;
   }

   @Override
   public String toString() {
      return "PersistenceConfigurationBuilder [stores=" + stores + ", attributes=" + attributes + "]";
   }
}
