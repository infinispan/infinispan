package org.infinispan.query.affinity;

import static java.util.stream.IntStream.range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.remoting.transport.Address;

/**
 * Provides shard distribution information for an index.
 *
 * @since 9.0
 */
interface ShardDistribution {

   /**
    * @return All shards identifiers.
    */
   Set<String> getShardsIdentifiers();

   /**
    * @return Owner for a single shard.
    */
   Address getOwner(String shardId);

   /**
    * @return All shards owned by a node.
    */
   Set<String> getShards(Address address);

   /**
    * @return the shard mapped to a certain segment
    */
   String getShardFromSegment(Integer segment);

   /**
    * @return input collection split into 'parts' sub collections
    */
   default <T> List<Set<T>> split(Collection<T> collection, int parts) {
      if (collection.isEmpty() || parts == 0) return Collections.emptyList();
      List<Set<T>> splits = new ArrayList<>(parts);
      range(0, parts).forEach(p -> splits.add(new HashSet<T>(collection.size() / parts)));
      int i = 0;
      for (T element : collection) {
         splits.get(i++ % parts).add(element);
      }
      return splits;
   }

}
