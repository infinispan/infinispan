package org.infinispan.query.affinity;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link ShardDistribution} that maintain a fixed number of index shards. The minimum number of shards is 1 and the
 * maximum is the number of segments.
 *
 * @since 9.0
 */
class FixedShardsDistribution implements ShardDistribution {

   private static final Log log = LogFactory.getLog(FixedShardsDistribution.class, Log.class);

   private final Map<Integer, String> shardPerSegmentMap = new HashMap<>();
   private final Map<Address, Set<String>> shardsPerAddressMap = new HashMap<>();
   private final Map<String, Address> addressPerShardMap = new HashMap<>();
   private final int numShards;

   FixedShardsDistribution(ConsistentHash consistentHash, int numShards) {
      if (numShards > consistentHash.getNumSegments()) {
         throw new IllegalArgumentException("Number of shards cannot be higher than number of segments");
      }
      if (numShards < 1) {
         throw new IllegalArgumentException("Minimum number of shards is 1");
      }
      this.numShards = numShards;
      calculate(consistentHash, numShards);
   }

   private void calculate(ConsistentHash consistentHash, int numShards) {
      List<Address> nodes = consistentHash.getMembers();
      int numNodes = nodes.size();

      List<Set<Integer>> segmentsPerServer = nodes.stream()
            .map(consistentHash::getPrimarySegmentsForOwner).collect(toList());

      int[] shardsNumPerServer = allocateShardsToNodes(numShards, numNodes, segmentsPerServer);
      this.populateSegments(shardsNumPerServer, segmentsPerServer, nodes);
      if (log.isTraceEnabled()) {
         log.tracef("Calculated shard distribution shardPerSegmentMap: %s", shardPerSegmentMap);
         log.tracef("Calculated shard distribution shardsPerAddressMap: %s", shardsPerAddressMap);
         log.tracef("Calculated shard distribution addressPerShardMap: %s", addressPerShardMap);
      }
   }

   /**
    * Associates segments to each shard.
    *
    * @param shardsNumPerServer numbers of shards allocated for each server
    * @param segmentsPerServer  the primary owned segments of each server
    * @param nodes              the members of the cluster
    */
   private void populateSegments(int[] shardsNumPerServer, List<Set<Integer>> segmentsPerServer, List<Address> nodes) {
      int shardId = 0;
      int n = 0;
      Set<Integer> remainingSegments = new HashSet<>();
      for (Address node : nodes) {
         Collection<Integer> primarySegments = segmentsPerServer.get(n);
         int shardQuantity = shardsNumPerServer[n];
         if (shardQuantity == 0) {
            remainingSegments.addAll(segmentsPerServer.get(n++));
            continue;
         }
         shardsPerAddressMap.computeIfAbsent(node, a -> new HashSet<>(shardQuantity));
         List<Set<Integer>> segments = split(primarySegments, shardsNumPerServer[n++]);
         for (Collection<Integer> shardSegments : segments) {
            String id = String.valueOf(shardId++);
            shardSegments.forEach(seg -> shardPerSegmentMap.put(seg, id));
            shardsPerAddressMap.get(node).add(id);
            addressPerShardMap.put(id, node);
         }
      }
      if (!remainingSegments.isEmpty()) {
         Iterator<String> shardIterator = Stream.iterate(0, i -> (i + 1) % numShards).map(String::valueOf).iterator();
         for (Integer segment : remainingSegments) {
            shardPerSegmentMap.put(segment, shardIterator.next());
         }
      }
   }

   /**
    * @return input collection split into 'parts' sub collections
    */
   private static List<Set<Integer>> split(Collection<Integer> segments, int parts) {
      if (segments.isEmpty() || parts == 0) return Collections.emptyList();
      List<Set<Integer>> splits = new ArrayList<>(parts);
      range(0, parts).forEach(p -> splits.add(new HashSet<>(segments.size() / parts)));
      int i = 0;
      for (Integer segment : segments) {
         splits.get(i++ % parts).add(segment);
      }
      return splits;
   }

   /**
    * Allocates shards in a round robin fashion for the servers, ignoring those without segments.
    *
    * @return int[] with the number of shards per server
    */
   private static int[] allocateShardsToNodes(int numShards, int numNodes, List<Set<Integer>> weightPerServer) {
      int[] shardsPerServer = new int[numNodes];
      Iterator<Integer> cyclicNodeIterator = Stream.iterate(0, i -> (i + 1) % numNodes).iterator();
      while (numShards > 0) {
         int slot = cyclicNodeIterator.next();
         if (!weightPerServer.get(slot).isEmpty()) {
            shardsPerServer[slot]++;
            numShards--;
         }
      }
      return shardsPerServer;
   }

   @Override
   public Set<String> getShardsIdentifiers() {
      return Collections.unmodifiableSet(addressPerShardMap.keySet());
   }

   @Override
   public Set<String> getShards(Address address) {
      return shardsPerAddressMap.get(address);
   }

   @Override
   public String getShardFromSegment(int segment) {
      return shardPerSegmentMap.get(segment);
   }

   @Override
   public Address getOwner(String shardId) {
      return addressPerShardMap.get(shardId);
   }
}
