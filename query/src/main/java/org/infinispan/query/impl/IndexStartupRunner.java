package org.infinispan.query.impl;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.query.Indexer;
import org.infinispan.search.mapper.mapping.SearchMapping;

public final class IndexStartupRunner {

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
      if (!IndexStartupMode.AUTO.equals(startupMode)) {
         return startupMode;
      }

      boolean dataIsVolatile = configuration.persistence().stores().stream()
            .allMatch(StoreConfiguration::purgeOnStartup);
      boolean indexesAreVolatile = IndexStorage.LOCAL_HEAP.equals(configuration.indexing().storage());

      if (dataIsVolatile && !indexesAreVolatile) {
         return IndexStartupMode.PURGE;
      }

      if (!dataIsVolatile && indexesAreVolatile) {
         return IndexStartupMode.REINDEX;
      }

      // if both (data and indexes) are volatile or not volatile they should be already aligned
      return IndexStartupMode.NONE;
   }
}
