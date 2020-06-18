package org.infinispan.persistence.remote.upgrade;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.impl.CacheTopologyInfoImpl;
import org.infinispan.persistence.remote.upgrade.impl.LocalityAwareDataPartitioner;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.LocationAwareDataPartitionTest", groups = "unit")
public class LocationAwareDataPartitionTest {

   private DataPartitioner partitioner = new LocalityAwareDataPartitioner();

   @Test
   public void testSplit() {
      for (int segments = 0; segments < 256; segments++) {
         for (int owners = 0; owners < 3; owners++) {
            for (int servers = 0; servers <= 10; servers++) {
               for (int partitionPerServer = servers; partitionPerServer <= 10; partitionPerServer++) {
                  test(segments, owners, servers, partitionPerServer);
               }
            }
         }
      }
   }

   private void test(int segments, int owners, int servers, int partitionsPerServer) {
      String message = String.format("Segments %d, owners %d, servers %d, partionsPerServer %d:", segments, owners,
            servers, partitionsPerServer);
      CacheTopologyInfo topology = createServerTopology(segments, owners, servers);
      Collection<Set<Integer>> result = partitioner.split(topology, partitionsPerServer);
      if (topology.getNumSegments() == 0 || topology.getSegmentsPerServer().size() == 0 || partitionsPerServer == 0) {
         assertEquals(0, result.size(), message);
      } else {
         assertTrue(result.stream().noneMatch(Collection::isEmpty), message);
         assertSegments(segments, result, message);
         assertLocality(topology, result, message);
      }
   }

   private void assertLocality(CacheTopologyInfo topology, Collection<Set<Integer>> result, String message) {
      // All partitions must be local to a server in the given topology, e.g., should be a subset of the
      // segments owned by a server
      for (Collection<Integer> partition : result) {
         boolean contained = topology.getSegmentsPerServer().values().stream().anyMatch(s -> s.containsAll(partition));
         if (!contained) {
            fail(message + " Partition " + partition + " is not local to any server ");
         }
      }
   }

   private void assertSegments(int segments, Collection<Set<Integer>> result, String message) {
      // All segments must be present in the produced partitions without duplication
      for (int s = 0; s < segments; s++) {
         boolean found = false;
         for (Collection<Integer> partitions : result) {
            if (partitions.contains(s)) {
               if (!found) {
                  found = true;
               } else {
                  fail("Duplicate segment found " + s);
               }
            }
         }
         if (!found) fail(message + " segment not found " + s);
      }
   }

   private CacheTopologyInfo createServerTopology(int numSegments, int numOwners, int numServers) {
      if (numSegments == 0 || numOwners == 0 || numServers == 0) return EmptyTopology.INSTANCE;

      List<SocketAddress> servers = new ArrayList<>();
      for (int i = 0; i < numServers; i++) {
         servers.add(InetSocketAddress.createUnresolved("server" + i, 0));
      }

      Map<SocketAddress, Set<Integer>> result = new HashMap<>();
      int serverCursor = 0;
      for (int seg = 0; seg < numSegments; seg++) {
         for (int owner = 0; owner < numOwners; owner++) {
            SocketAddress socketAddress = servers.get(serverCursor);
            result.computeIfAbsent(socketAddress, s -> new HashSet<>()).add(seg);

            if (++serverCursor == numServers) serverCursor = 0;
         }
      }
      return new CacheTopologyInfoImpl(result, numSegments, 0);
   }

   private static class EmptyTopology implements CacheTopologyInfo {
      static final EmptyTopology INSTANCE = new EmptyTopology();

      @Override
      public int getNumSegments() {
         return 0;
      }

      @Override
      public Map<SocketAddress, Set<Integer>> getSegmentsPerServer() {
         return new HashMap<>();
      }

      @Override
      public Integer getTopologyId() {
         return 0;
      }
   }

}
