package org.infinispan.query.impl.massindex;

import java.util.Set;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.indexmanager.InfinispanIndexManager;

/**
 * @author gustavonalle
 * @since 8.2
 */
final class MassIndexStrategyFactory {

   private MassIndexStrategyFactory() {
   }

   static MassIndexStrategy calculateStrategy(EntityIndexBinding indexBinding, Configuration cacheConfiguration) {
      Set<IndexManager> indexManagers = indexBinding.getIndexManagerSelector().all();
      Index index = cacheConfiguration.indexing().index();
      IndexManager indexManager = indexManagers.iterator().next();

      boolean sharded = indexManagers.size() > 1;
      boolean replicated = cacheConfiguration.clustering().cacheMode().isReplicated();
      boolean singleMaster = !sharded && indexManager instanceof InfinispanIndexManager;
      boolean custom = !(indexManager instanceof DirectoryBasedIndexManager);

      if (singleMaster || custom) {
         return MassIndexStrategy.SHARED_INDEX_STRATEGY;
      }

      if (sharded || replicated || !index.isPrimaryOwner()) {
         return MassIndexStrategy.PER_NODE_ALL_DATA;
      }

      return MassIndexStrategy.PER_NODE_PRIMARY;

   }
}
