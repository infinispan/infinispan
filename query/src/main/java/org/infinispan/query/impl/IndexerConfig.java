package org.infinispan.query.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.IndexWriterConfiguration;
import org.infinispan.configuration.cache.PrivateIndexingConfiguration;

public class IndexerConfig {

   private final int maxConcurrency;
   private final int rebatchRequestsSize;

   public IndexerConfig(AdvancedCache<?, ?> cache) {
      Configuration config = cache.getCacheConfiguration();
      IndexWriterConfiguration writer = config.indexing().writer();
      Integer queueSize = writer.getQueueSize();
      Integer queueCount = writer.getQueueCount();

      // we **guess** that the distribution of the hash functions applied to the document id as keys to be 67% of the optimal
      maxConcurrency = queueCount == 1 ? queueSize : (int) (queueCount * queueSize * 0.67);

      PrivateIndexingConfiguration privateConfig = config.module(PrivateIndexingConfiguration.class);
      if (privateConfig != null) {
         rebatchRequestsSize = privateConfig.rebatchRequestsSize();
      } else {
         rebatchRequestsSize = 10_000;
      }
   }

   public IndexerConfig(int maxConcurrency, int rebatchRequestsSize) {
      this.maxConcurrency = maxConcurrency;
      this.rebatchRequestsSize = rebatchRequestsSize;
   }

   public int maxConcurrency() {
      return maxConcurrency;
   }

   public int rebatchRequestsSize() {
      return rebatchRequestsSize;
   }
}
