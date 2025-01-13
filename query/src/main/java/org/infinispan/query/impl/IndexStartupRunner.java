package org.infinispan.query.impl;

import java.util.List;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.query.Indexer;
import org.infinispan.search.mapper.log.impl.Log;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.util.logging.LogFactory;

public final class IndexStartupRunner {

   private static final Log log = LogFactory.getLog(IndexStartupRunner.class, Log.class);

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

      if (DataKind.SHARED_STORE_EVICTION_ENABLED.equals(dataKind) && !indexesAreVolatile &&
            IndexStartupMode.AUTO.equals(startupMode)) {
         // in this case a node that is crashed could have not aligned indexes.
         // we force the reindex when it recovers to be sure that is aligned.
         return IndexStartupMode.REINDEX;
      }

      return (IndexStartupMode.AUTO.equals(startupMode)) ? IndexStartupMode.NONE : startupMode;
   }

   private enum DataKind {
      VOLATILE, PERSISTENT, SHARED_STORE_EVICTION_ENABLED
   }

   private static DataKind computeDataKind(Configuration configuration) {
      List<StoreConfiguration> cacheStores = configuration.persistence().stores();
      if (cacheStores.isEmpty()) {
         return DataKind.VOLATILE;
      }

      boolean sharedStoreEvictionEnabled = false;
      for (StoreConfiguration cacheStore : cacheStores) {
         if (cacheStore.purgeOnStartup()) {
            continue;
         }
         if (cacheStore.shared() && configuration.memory().isEvictionEnabled()) {
            sharedStoreEvictionEnabled = true;
         } else {
            // priority rule: if a non-shared cache store is present the cache is persistent
            return DataKind.PERSISTENT;
         }
      }

      if (sharedStoreEvictionEnabled) {
         // if no non-shared cache store is present and a shared cache store is present and the eviction is enabled,
         // we're in the case of shared memory with eviction enabled
         return DataKind.SHARED_STORE_EVICTION_ENABLED;
      }
      return DataKind.VOLATILE;
   }
}
