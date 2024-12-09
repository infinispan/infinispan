package org.infinispan.query.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.search.util.common.logging.impl.LoggerFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.query.Indexer;
import org.infinispan.search.mapper.log.impl.Log;
import org.infinispan.search.mapper.mapping.SearchMapping;

public final class IndexStartupRunner {

   private static final Log log = LoggerFactory.make(Log.class, MethodHandles.lookup());

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
      boolean dataIsVolatile = configuration.persistence().stores().stream()
            .allMatch(StoreConfiguration::purgeOnStartup);
      boolean indexesAreVolatile = IndexStorage.LOCAL_HEAP.equals(configuration.indexing().storage());
      if (dataIsVolatile && !indexesAreVolatile) {
         switch (startupMode) {
            case AUTO, PURGE -> {
               return IndexStartupMode.PURGE;
            }
            case REINDEX -> {
               // equivalent to purge, since there is no data in the caches
               return IndexStartupMode.REINDEX;
            }
            default -> {
               log.volatileDataPersistentIndexesStartupModeNone();
               return IndexStartupMode.NONE;
            }
         }
      }
      if (!dataIsVolatile && indexesAreVolatile) {
         switch (startupMode) {
            case AUTO, REINDEX -> {
               return IndexStartupMode.REINDEX;
            }
            default -> {
               log.volatileDataPersistentIndexesStartupModeNone();
               return startupMode;
            }
         }
      }
      return (IndexStartupMode.AUTO.equals(startupMode)) ? IndexStartupMode.NONE : startupMode;
   }
}
