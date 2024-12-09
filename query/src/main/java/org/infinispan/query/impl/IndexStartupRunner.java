package org.infinispan.query.impl;

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
      boolean dataIsVolatile = configuration.persistence().stores().stream()
            .allMatch(StoreConfiguration::purgeOnStartup);
      boolean indexesAreVolatile = IndexStorage.LOCAL_HEAP.equals(configuration.indexing().storage());
      if (dataIsVolatile && !indexesAreVolatile) {
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
      if (!dataIsVolatile && indexesAreVolatile) {
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
      return (IndexStartupMode.AUTO.equals(startupMode)) ? IndexStartupMode.NONE : startupMode;
   }
}
