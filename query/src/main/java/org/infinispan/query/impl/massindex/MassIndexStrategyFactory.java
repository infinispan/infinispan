package org.infinispan.query.impl.massindex;

import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.query.affinity.ShardIndexManager;
import org.infinispan.query.indexmanager.InfinispanIndexManager;

/**
 * @author gustavonalle
 * @since 8.2
 */
final class MassIndexStrategyFactory {

   private MassIndexStrategyFactory() {
   }

   static MassIndexStrategy calculateStrategy(EntityIndexBinding indexBinding, Configuration cacheConfiguration) {
      IndexManager[] indexManagers = indexBinding.getIndexManagers();
      IndexManager indexManager = indexBinding.getIndexManagers()[0];

      boolean sharded = indexManagers.length > 1;
      boolean replicated = cacheConfiguration.clustering().cacheMode().isReplicated();
      boolean singleMaster = !sharded && indexManager instanceof InfinispanIndexManager;
      boolean multiMaster = indexManager instanceof ShardIndexManager;
      boolean custom = !(indexManager instanceof DirectoryBasedIndexManager);

      if (singleMaster || custom) {
         return MassIndexStrategy.SHARED_INDEX_STRATEGY;
      }

      if (multiMaster) {
         return MassIndexStrategy.PER_NODE_PRIMARY;
      }

      if (sharded || replicated) {
         return MassIndexStrategy.PER_NODE_ALL_DATA;
      }

      return MassIndexStrategy.PER_NODE_PRIMARY;

   }
}
