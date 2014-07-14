package org.infinispan.configuration.cache;

import org.infinispan.commons.util.TypedProperties;

/**
 * Holds default configurations about indexing
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
         properties.putIfAbsent(Constants.DIRECTORY_PROVIDER, "filesystem");
         properties.putIfAbsent(Constants.EXCLUSIVE_INDEX_USE, "true");
         properties.putIfAbsent(Constants.INDEX_MANAGER, "near-real-time");
         properties.putIfAbsent(Constants.READER_STRATEGY, "shared");
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
         properties.putIfAbsent(Constants.DIRECTORY_PROVIDER, "infinispan");
         properties.putIfAbsent(Constants.EXCLUSIVE_INDEX_USE, "true");
         properties.putIfAbsent(Constants.INDEX_MANAGER, "org.infinispan.query.indexmanager.InfinispanIndexManager");
         properties.putIfAbsent(Constants.READER_STRATEGY, "shared");
      }

   };

   /**
    * Applies pre-defined configurations
    *
    * @param properties existing Properties
    */
   abstract void apply(TypedProperties properties);

   private static class Constants {
      static final String DIRECTORY_PROVIDER = "hibernate.search.default.directory_provider";
      static final String EXCLUSIVE_INDEX_USE = "hibernate.search.default.exclusive_index_use";
      static final String INDEX_MANAGER = "hibernate.search.default.indexmanager";
      static final String READER_STRATEGY = "hibernate.search.default.reader.strategy";
   }
}
