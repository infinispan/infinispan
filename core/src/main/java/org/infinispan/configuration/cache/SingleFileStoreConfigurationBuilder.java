package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.FRAGMENTATION_FACTOR;
import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.LOCATION;
import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.MAX_ENTRIES;

import org.infinispan.commons.configuration.Builder;
/**
 * Single file cache store configuration builder.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class SingleFileStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<SingleFileStoreConfiguration, SingleFileStoreConfigurationBuilder> {


   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, SingleFileStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public SingleFileStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets a location on disk where the store can write.
    */
   public SingleFileStoreConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
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
      attributes.attribute(MAX_ENTRIES).set(maxEntries);
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
      attributes.attribute(FRAGMENTATION_FACTOR).set(fragmentationFactor);
      return this;
   }

   @Override
   public SingleFileStoreConfiguration create() {
      return new SingleFileStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(SingleFileStoreConfiguration template) {
      super.read(template);
      return this;
   }

}
