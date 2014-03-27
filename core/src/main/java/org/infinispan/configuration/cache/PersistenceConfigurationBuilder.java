package org.infinispan.configuration.cache;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationUtils;
import org.infinispan.commons.CacheConfigurationException;

/**
 * Configuration for cache stores.
 *
 */
public class PersistenceConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<PersistenceConfiguration> {

   private boolean passivation = false;
   private List<StoreConfigurationBuilder<?,?>> stores = new ArrayList<StoreConfigurationBuilder<?,?>>(2);

   protected PersistenceConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public PersistenceConfigurationBuilder passivation(boolean b) {
      this.passivation = b;
      return this;
   }

   /**
    * If true, data is only written to the cache store when it is evicted from memory, a phenomenon
    * known as 'passivation'. Next time the data is requested, it will be 'activated' which means
    * that data will be brought back to memory and removed from the persistent store. This gives you
    * the ability to 'overflow' to disk, similar to swapping in an operating system. <br />
    * <br />
    * If false, the cache store contains a copy of the contents in memory, so writes to cache result
    * in cache store writes. This essentially gives you a 'write-through' configuration.
    */
   boolean passivation() {
      return passivation;
   }

   /**
    * Adds a cache loader which uses the specified builder class to build its configuration
    */
   public <T extends StoreConfigurationBuilder<?, ?>> T addStore(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(PersistenceConfigurationBuilder.class);
         T builder = constructor.newInstance(this);
         this.stores.add(builder);
         return builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate loader configuration builder '" + klass.getName()
               + "'", e);
      }
   }

   /**
    * Adds a cache loader which uses the specified builder instance to build its configuration
    *
    * @param builder an instance of {@link StoreConfigurationBuilder}
    */
   public StoreConfigurationBuilder<?, ?> addStore(StoreConfigurationBuilder<?, ?> builder) {
      this.stores.add(builder);
      return builder;
   }

   /**
    * Adds a cluster cache loader
    */
   public ClusterLoaderConfigurationBuilder addClusterLoader() {
      ClusterLoaderConfigurationBuilder builder = new ClusterLoaderConfigurationBuilder(this);
      this.stores.add(builder);
      return builder;
   }

   /**
    * Adds a single file cache store
    */
   public SingleFileStoreConfigurationBuilder addSingleFileStore() {
      SingleFileStoreConfigurationBuilder builder = new SingleFileStoreConfigurationBuilder(this);
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
      int numFetchPersistentState = 0;
      for (StoreConfigurationBuilder<?, ?> b : stores) {
         b.validate();
         StoreConfiguration storeConfiguration = b.create();
         if (storeConfiguration.shared() && storeConfiguration.singletonStore().enabled()) {
            throw new CacheConfigurationException("Invalid cache loader configuration for " + storeConfiguration.getClass().getSimpleName()
                                                        + "  If a cache loader is configured as a singleton, the cache loader cannot be shared in a cluster!");
         }
         if (storeConfiguration.fetchPersistentState())
            numFetchPersistentState++;
      }
      if (numFetchPersistentState > 1)
         throw new CacheConfigurationException("Maximum one store can be set to 'fetchPersistentState'!");
   }

   @Override
   public PersistenceConfiguration create() {
      List<StoreConfiguration> stores = new ArrayList<StoreConfiguration>(this.stores.size());
      for (StoreConfigurationBuilder<?, ?> loader : this.stores)
         stores.add(loader.create());
      return new PersistenceConfiguration(passivation, stores);
   }

   @SuppressWarnings("unchecked")
   @Override
   public PersistenceConfigurationBuilder read(PersistenceConfiguration template) {
      clearStores();
      for (StoreConfiguration c : template.stores()) {
         Class<? extends StoreConfigurationBuilder<?, ?>> builderClass = (Class<? extends StoreConfigurationBuilder<?, ?>>) ConfigurationUtils.builderForNonStrict(c);
         if (builderClass == null) {
            builderClass = CustomStoreConfigurationBuilder.class;
         }
         StoreConfigurationBuilder builder =  this.addStore(builderClass);
         builder.read(c);
      }
      this.passivation = template.passivation();
      return this;
   }

   public List<StoreConfigurationBuilder<?, ?>> stores() {
      return stores;
   }

   @Override
   public String toString() {
      return "PersistenceConfigurationBuilder{" +
            "stores=" + stores +
            ", passivation=" + passivation +
            '}';
   }

}
