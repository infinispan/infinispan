package org.infinispan.query.impl;

import java.util.List;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.impl.Log;
import org.infinispan.query.mapper.mapping.SearchMapping;

public final class IndexStartupRunner {

   private static final Log log = Log.getLog(IndexStartupRunner.class);

   public static void run(SearchMapping mapping, Indexer indexer, Configuration configuration) {
      IndexStartupMode startupMode = computeFinalMode(configuration);

      if (IndexStartupMode.PURGE.equals(startupMode)) {
         mapping.scopeAll().workspace().purge();
      } else if (IndexStartupMode.REINDEX.equals(startupMode)) {
         indexer.runLocal();
      }
   }

   private IndexStartupRunner() {
   }

   private static IndexStartupMode computeFinalMode(Configuration configuration) {
      IndexStartupMode startupMode = configuration.indexing().startupMode();
      DataKind dataKind = computeDataKind(configuration);
      boolean indexesAreVolatile = IndexStorage.LOCAL_HEAP.equals(configuration.indexing().storage());

      if (DataKind.VOLATILE.equals(dataKind) && !indexesAreVolatile) {
         switch (startupMode) {
            case AUTO, PURGE,
                 // reindex is equivalent to purge, since there is no data in the caches
                 REINDEX -> {
               return IndexStartupMode.PURGE;
            }
            default -> {
               log.logIndexStartupModeMismatch("volatile", "persistent", startupMode.toString());
               return IndexStartupMode.NONE;
            }
         }
      }

      if (DataKind.PERSISTENT.equals(dataKind) && indexesAreVolatile) {
         switch (startupMode) {
            case AUTO, REINDEX -> {
               return IndexStartupMode.REINDEX;
            }
            default -> {
               log.logIndexStartupModeMismatch("persistent", "volatile", startupMode.toString());
               return startupMode;
            }
         }
      }

      if (DataKind.SHARED_STORE.equals(dataKind) && !indexesAreVolatile &&
            IndexStartupMode.AUTO.equals(startupMode)) {
         // @fax4ever: I'm against this. In my opinion run always a reindex, even if
         // configuration.memory().isEvictionEnabled() is false, is wrong.
         // This may penalize the average user using a shared cache store that does not do any eviction!
         // But since the others of the team want this at all cost, I give up in the spirit of collaboration.
         return IndexStartupMode.REINDEX;
      }

      return (IndexStartupMode.AUTO.equals(startupMode)) ? IndexStartupMode.NONE : startupMode;
   }

   private enum DataKind {
      VOLATILE, PERSISTENT, SHARED_STORE
   }

   private static DataKind computeDataKind(Configuration configuration) {
      List<StoreConfiguration> cacheStores = configuration.persistence().stores();
      if (cacheStores.isEmpty()) {
         return DataKind.VOLATILE;
      }

      boolean sharedStore = false;
      for (StoreConfiguration cacheStore : cacheStores) {
         if (cacheStore.purgeOnStartup()) {
            continue;
         }
         if (cacheStore.shared()) {
            // @fax4ever: I'm against this. In my opinion run always a reindex, even if
            // configuration.memory().isEvictionEnabled() is false, is wrong.
            // This may penalize the average user using a shared cache store that does not do any eviction!
            // But since the others of the team want this at all cost, I give up in the spirit of collaboration.
            sharedStore = true;
         } else {
            return DataKind.PERSISTENT;
         }
      }

      if (sharedStore) {
         return DataKind.SHARED_STORE;
      }
      return DataKind.VOLATILE;
   }
}
