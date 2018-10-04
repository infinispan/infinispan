package org.infinispan.query.affinity;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "query.ShardDistributionTest")
public class ShardDistributionTest {

   private static final int MAX_SEGMENTS = 64;
   private static final int MAX_NODES = 3;

   @Test
   public void testAllocation() throws Exception {
      for (int nodes = 1; nodes < MAX_NODES; nodes++) {
         for (int segments = 1; segments < MAX_SEGMENTS; segments++) {
            for (int shards = 1; shards <= segments; shards++) {
               assertAllocation(nodes, segments, shards);
            }
         }
      }
   }

   @Test
   public void testDistributionWithNodesOwningNoSegments() throws Exception {
      TestAddress segmentsOwned = new TestAddress(1);
      TestAddress noSegmentOwned = new TestAddress(2);
      List<Address> members = Arrays.asList(segmentsOwned, noSegmentOwned);
      int segments = 256;
      int shards = 8;

      List<Address>[] owners = new List[segments];
      range(0, segments).forEach(s -> owners[s] = singletonList(segmentsOwned));
      ConsistentHash ch = new DefaultConsistentHash(MurmurHash3.getInstance(), 2, segments, members, null, owners);

      ShardDistribution shardDistribution = new FixedShardsDistribution(ch, shards);

      assertEquals(8, shardDistribution.getShardsIdentifiers().size());
      assertEquals(8, shardDistribution.getShards(segmentsOwned).size());
      assertTrue(shardDistribution.getShards(noSegmentOwned) == null);
   }

   private static void assertAllocation(int numberOfNodes, int numberOfSegments, int numberOfShards) {
      ConsistentHash ch = createConsistentHash(numberOfSegments, numberOfNodes);

      FixedShardsDistribution shardDistribution = new FixedShardsDistribution(ch, numberOfShards);

      Set<String> allShards = shardDistribution.getShardsIdentifiers();
      Set<String> shardsFromSegments = range(0, numberOfSegments).boxed()
            .map(shardDistribution::getShardFromSegment)
            .collect(Collectors.toSet());

      assertEquals(numberOfShards, shardsFromSegments.size());
      assertEquals(numberOfShards, allShards.size());
   }

   private static ConsistentHash createConsistentHash(int numSegments, int numNodes) {
      return createConsistentHash(numSegments, numNodes, 1);
   }

   private static ConsistentHash createConsistentHash(int numSegments, int numNodes, int numOwners) {
      if (numSegments == 0) {
         return null;
      }
      List<Address> servers = range(0, numNodes).boxed().map(TestAddress::new).collect(toList());
      List<Address>[] owners = new List[numSegments];

      range(0, numSegments).boxed().forEach(segment -> {
         Collections.rotate(servers, 1);
         owners[segment] = new ArrayList<>(servers.subList(0, numOwners));
      });

      return new DefaultConsistentHash(MurmurHash3.getInstance(), numOwners, numSegments, servers, null, owners);
   }
}
