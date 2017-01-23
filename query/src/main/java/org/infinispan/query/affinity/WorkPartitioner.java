package org.infinispan.query.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.search.backend.LuceneWork;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * Partitions {@link LuceneWork} based on the location it should be applied.
 *
 * @since 9.0
 */
class WorkPartitioner {

   private static final Log log = LogFactory.getLog(WorkPartitioner.class, Log.class);

   private final AffinityIndexManager affinityIndexManager;
   private final ShardAllocatorManager shardAllocatorManager;
   private final ShardAddress localShardAddress;
   private final String indexName;

   WorkPartitioner(AffinityIndexManager affinityIndexManager, ShardAllocatorManager shardAllocatorManager) {
      this.indexName = affinityIndexManager.getIndexName();
      this.localShardAddress = affinityIndexManager.getLocalShardAddress();
      this.affinityIndexManager = affinityIndexManager;
      this.shardAllocatorManager = shardAllocatorManager;
   }

   Map<ShardAddress, List<LuceneWork>> partitionWorkByAddress(Collection<LuceneWork> works, boolean originLocal,
                                                              boolean isRetry) {
      return works.stream().collect(
            Collectors.toMap(w -> this.getLocation(w, originLocal, isRetry), WorkPartitioner::newList, (w1, w2) -> {
               w1.addAll(w2);
               return w1;
            })
      );
   }

   private static String extractShardName(String indexName) {
      int idx = indexName.lastIndexOf('.');
      return idx == -1 ? "0" : indexName.substring(idx + 1);
   }

   private String getIndexName(String shardId) {
      return indexName.substring(0, indexName.lastIndexOf('.') + 1) + shardId;
   }

   @SafeVarargs
   private static <T> List<T> newList(T... elements) {
      List<T> list = new ArrayList<>(elements.length);
      Collections.addAll(list, elements);
      return list;
   }

   private ShardAddress getLocation(LuceneWork work, boolean originLocal, boolean isRetry) {
      log.debugf("Getting location for work %s at %s", work, localShardAddress);
      if (work.getIdInString() == null) {
         String shard = String.valueOf(extractShardName(indexName));
         Address destination = isRetry ? affinityIndexManager.getLockHolder() : shardAllocatorManager.getOwner(shard);
         return new ShardAddress(shard, destination);
      }
      Object workKey = affinityIndexManager.stringToKey(work.getIdInString());
      String workShard = shardAllocatorManager.getShardFromKey(workKey);
      String workIndexName = this.getIndexName(workShard);
      Address workOwner = shardAllocatorManager.getOwner(workShard);

      if (isRetry || !originLocal) {
         Address lockHolder = affinityIndexManager.getLockHolder(workIndexName, workShard);
         if (lockHolder != null && !lockHolder.equals(localShardAddress.getAddress())) {
            return new ShardAddress(null, lockHolder);
         }
         return workIndexName.equals(indexName) ? localShardAddress : new ShardAddress(workIndexName, lockHolder);
      }

      if (workOwner.equals(localShardAddress.getAddress())) {
         return workIndexName.equals(indexName) ? localShardAddress :
               new ShardAddress(workIndexName, localShardAddress.getAddress());
      } else {
         return new ShardAddress(null, workOwner);
      }
   }

}
