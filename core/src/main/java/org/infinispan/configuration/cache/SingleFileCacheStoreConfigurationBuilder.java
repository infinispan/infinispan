package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Single file cache store configuration builder.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class SingleFileCacheStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<SingleFileCacheStoreConfiguration, SingleFileCacheStoreConfigurationBuilder> {

   private static final Log log = LogFactory.getLog(SingleFileCacheStoreConfigurationBuilder.class);

   private String location = "Infinispan-SingleFileCacheStore";

   private int maxEntries = -1;

   public SingleFileCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public SingleFileCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets a location on disk where the store can write.
    */
   public SingleFileCacheStoreConfigurationBuilder location(String location) {
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
   public SingleFileCacheStoreConfigurationBuilder maxEntries(int maxEntries) {
      this.maxEntries = maxEntries;
      return this;
   }

   @Override
   public SingleFileCacheStoreConfiguration create() {
      return new SingleFileCacheStoreConfiguration(location, maxEntries,
            purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
            ignoreModifications, TypedProperties.toTypedProperties(properties),
            async.create(), singletonStore.create());
   }

   @Override
   public Builder<?> read(SingleFileCacheStoreConfiguration template) {
      // SingleFileCacheStore-specific configuration
      location = template.location();
      maxEntries = template.maxEntries();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

}
