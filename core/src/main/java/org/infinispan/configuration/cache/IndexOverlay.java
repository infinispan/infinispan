package org.infinispan.configuration.cache;

import org.infinispan.commons.util.TypedProperties;

/**
 * Holds indexing default configurations to be used for auto-configuration.
 *
 * @author gustavonalle
 * @since 7.0
 */
enum IndexOverlay {

   /**
    * Default for replicated cache: filesystem based (so it'll likely be a mmap index on Unix),
    * near-real-time, exclusive use, reuse index reader across queries
    */
   NON_DISTRIBUTED_FS {
      @Override
      void apply(TypedProperties properties) {
         properties.putIfAbsent(DIRECTORY_PROVIDER, "filesystem");
         properties.putIfAbsent(EXCLUSIVE_INDEX_USE, "true");
         properties.putIfAbsent(INDEX_MANAGER, "near-real-time");
         properties.putIfAbsent(READER_STRATEGY, "shared");
      }
   },

   /**
    * Default for indexing a distributed cache. It will store indexes
    * in Infinispan itself, with a master/slave backend in order to
    * avoid holding cluster-wide locking during indexing
    */
   DISTRIBUTED_INFINISPAN {
      @Override
      void apply(TypedProperties properties) {
         properties.putIfAbsent(DIRECTORY_PROVIDER, "infinispan");
         properties.putIfAbsent(EXCLUSIVE_INDEX_USE, "true");
         properties.putIfAbsent(INDEX_MANAGER, "org.infinispan.query.indexmanager.InfinispanIndexManager");
         properties.putIfAbsent(READER_STRATEGY, "shared");
      }
   };

   private static final String DIRECTORY_PROVIDER = "hibernate.search.default.directory_provider";
   private static final String EXCLUSIVE_INDEX_USE = "hibernate.search.default.exclusive_index_use";
   private static final String INDEX_MANAGER = "hibernate.search.default.indexmanager";
   private static final String READER_STRATEGY = "hibernate.search.default.reader.strategy";

   /**
    * Applies pre-defined configurations.
    *
    * @param properties existing TypedProperties
    */
   abstract void apply(TypedProperties properties);
}
