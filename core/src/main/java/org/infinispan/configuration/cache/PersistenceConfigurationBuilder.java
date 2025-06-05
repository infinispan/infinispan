package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.PersistenceConfiguration.AVAILABILITY_INTERVAL;
import static org.infinispan.configuration.cache.PersistenceConfiguration.CONNECTION_ATTEMPTS;
import static org.infinispan.configuration.cache.PersistenceConfiguration.PASSIVATION;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;

/**
 * Configuration for cache stores.
 *
 */
public class PersistenceConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<PersistenceConfiguration> {
   private final List<StoreConfigurationBuilder<?, ?>> stores = new ArrayList<>(2);
   private final AttributeSet attributes;

   protected PersistenceConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = PersistenceConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public PersistenceConfigurationBuilder passivation(boolean b) {
      attributes.attribute(PASSIVATION).set(b);
      return this;
   }

   /**
    * @param interval The time, in milliseconds, between availability checks to determine if the PersistenceManager
    *           is available. In other words, this interval sets how often stores/loaders are polled via their
    *           `org.infinispan.persistence.spi.CacheWriter#isAvailable` or
    *           `org.infinispan.persistence.spi.CacheLoader#isAvailable` implementation. If a single store/loader is
    *           not available, an exception is thrown during cache operations.
    */
   public PersistenceConfigurationBuilder availabilityInterval(int interval) {
      attributes.attribute(AVAILABILITY_INTERVAL).set(TimeQuantity.valueOf(interval));
      return this;
   }

   /**
    * Same as {@link #availabilityInterval(int)} but supporting time units
    */
   public PersistenceConfigurationBuilder availabilityInterval(String interval) {
      attributes.attribute(AVAILABILITY_INTERVAL).set(TimeQuantity.valueOf(interval));
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
   @Deprecated(forRemoval=true)
   public PersistenceConfigurationBuilder connectionInterval(int interval) {
      // Ignore
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
    * Adds a single file cache store.
    * @deprecated since 13.0. To be removed in 16.0 has been replaced by {@link #addSoftIndexFileStore()}
    */
   @Deprecated(forRemoval=true, since = "13.0")
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
         if (storeConfiguration.preload()) {
            numPreload++;
         }
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
      List<StoreConfiguration> stores = new ArrayList<>(this.stores.size());
      for (StoreConfigurationBuilder<?, ?> loader : this.stores)
         stores.add(loader.create());
      return new PersistenceConfiguration(attributes.protect(), stores);
   }

   @SuppressWarnings("unchecked")
   @Override
   public PersistenceConfigurationBuilder read(PersistenceConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      if (combine.repeatedAttributes() == Combine.RepeatedAttributes.OVERRIDE && template.attributes().isTouched()) {
         clearStores();
      }
      for (StoreConfiguration c : template.stores()) {
         Class<? extends StoreConfigurationBuilder<?, ?>> builderClass = getBuilderClass(c);
         StoreConfigurationBuilder builder =  this.getBuilderFromClass(builderClass);
         stores.add((StoreConfigurationBuilder) builder.read(c, combine));
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
