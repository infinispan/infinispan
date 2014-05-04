package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

/**
 * Single file cache store configuration builder.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class SingleFileStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<SingleFileStoreConfiguration, SingleFileStoreConfigurationBuilder> {

   private String location = "Infinispan-SingleFileStore";

   private int maxEntries = -1;

   private float fragmentationFactor  = 0.75f;

   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public SingleFileStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets a location on disk where the store can write.
    */
   public SingleFileStoreConfigurationBuilder location(String location) {
      this.location = location;
      return this;
   }

   /**
    * In order to speed up lookups, the single file cache store keeps an index
    * of keys and their corresponding position in the file. To avoid this
    * index resulting in memory consumption problems, this cache store can
    * bounded by a maximum number of entries that it stores. If this limit is
    * exceeded, entries are removed permanently using the LRU algorithm both
    * from the in-memory index and the underlying file based cache store.
    *
    * So, setting a maximum limit only makes sense when Infinispan is used as
    * a cache, whose contents can be recomputed or they can be retrieved from
    * the authoritative data store.
    *
    * If this maximum limit is set when the Infinispan is used as an
    * authoritative data store, it could lead to data loss, and hence it's
    * not recommended for this use case.
    */
   public SingleFileStoreConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   /**
    * The store tries to fit in a new entry into an existing entry from a free entry pool (if one is available)
    * However, this existing free entry may be quite bigger than what is required to contain the new entry
    * It may then make sense to split the free entry into two parts:
    * 1. That is required to contain the new entry requested
    * 2. the remaining part to be returned to the pool of free entries.
    * The fragmentationFactor decides when to split the free entry.
    * So, if this value is set as 0.75, then the free entry will be split if the new entry is equal to or less than 0.75 times the size of free entry
    */
   public SingleFileStoreConfigurationBuilder fragmentationFactor(float fragmentationFactor) {
      this.fragmentationFactor  = fragmentationFactor;
      return this;
   }

   @Override
   public SingleFileStoreConfiguration create() {
      return new SingleFileStoreConfiguration(purgeOnStartup, fetchPersistentState,ignoreModifications,
                                                    async.create(), singletonStore.create(), preload,
                                                    shared, properties, location, maxEntries, fragmentationFactor);
   }

   @Override
   public Builder<?> read(SingleFileStoreConfiguration template) {
      super.read(template);

      // SingleFileStore-specific configuration
      location = template.location();
      maxEntries = template.maxEntries();
      fragmentationFactor  = template.fragmentationFactor();

      return this;
   }

}
