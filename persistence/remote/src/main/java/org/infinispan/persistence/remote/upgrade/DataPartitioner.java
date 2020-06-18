package org.infinispan.persistence.remote.upgrade;

import java.util.Collection;
import java.util.Set;

import org.infinispan.client.hotrod.CacheTopologyInfo;

/**
 * Creates data partitions to allow parallel processing. Each data partition is defined as a list of segments, thus
 * no segments can be in more than one partition and all the partitions together must cover all segments.
 */
public interface DataPartitioner {

   /**
    * @param sourceClusterTopology the {@link CacheTopologyInfo} of a cluster where data is located.
    * @param partitionsPerServer how many partitions to produce per each server. In some circumstances, it may not be
    *                        possible to honor this param, for e.g., when there aren't enough segments compared to the
    *                        number of servers.
    * @return a collection of data partitions
    */
   Collection<Set<Integer>> split(CacheTopologyInfo sourceClusterTopology, int partitionsPerServer);


   /**
    * Same as {@link #split(CacheTopologyInfo, int)} but will create a single partition per server
    */
   default Collection<Set<Integer>> split(CacheTopologyInfo sourceClusterTopology) {
      return split(sourceClusterTopology, 1);
   }

}
