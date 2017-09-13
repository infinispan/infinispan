package org.infinispan.server.test.partitionhandling;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.PartitionHandlingController;
import org.infinispan.server.test.util.StandaloneManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests partition handling strategies on a 3 node cluster, which is split into 2 partitions: (node0, node1) and (node2).
 * We pick 2 keys that have as owners node0 and node2, and then try reading them from each partition.
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "partitionhandling-1"), @RunningServer(name = "partitionhandling-2"), @RunningServer(name = "partitionhandling-3")})
public class PartitionHandlingIT {

   final String CONTAINER1 = "partitionhandling-1";
   final String CONTAINER2 = "partitionhandling-2";
   final String CONTAINER3 = "partitionhandling-3";

   final static int NUM_SEGMENTS = 6;
   final static String DENY_READ_WRITES_CACHE = "denyreadwrites";
   final static String ALLOW_READS_CACHE = "allowreads";
   final static String ALLOW_READS_CACHE_2_OWNERS = "allowreads_2owners";
   final static String ALLOW_READ_WRITES_CACHE = "allowreadwrites";

   PartitionHandlingController partitionHandlingController;

   @InfinispanResource(CONTAINER1)
   RemoteInfinispanServer server1;
   String server1Address;

   @InfinispanResource(CONTAINER2)
   RemoteInfinispanServer server2;

   @InfinispanResource(CONTAINER3)
   RemoteInfinispanServer server3;
   String server3Address;

   @Before
   public void setUp() {
      server1Address = server1.getHotrodEndpoint().getInetAddress().getHostAddress() + ":" + server1.getHotrodEndpoint().getPort();
      server3Address = server3.getHotrodEndpoint().getInetAddress().getHostAddress() + ":" + server3.getHotrodEndpoint().getPort();

      if (partitionHandlingController == null) {
         String node0Ip = System.getProperty("node0.ip");
         String node1Ip = System.getProperty("node1.ip");
         String node2Ip = System.getProperty("node2.ip");
         int node0Port = Integer.valueOf(System.getProperty("node0.mgmt.port"));
         int node1Port = Integer.valueOf(System.getProperty("node1.mgmt.port"));
         int node2Port = Integer.valueOf(System.getProperty("node2.mgmt.port"));

         StandaloneManagementClient[] clients = new StandaloneManagementClient[3];
         clients[0] = new StandaloneManagementClient(node0Ip, node0Port, "node0");
         clients[1] = new StandaloneManagementClient(node1Ip, node1Port, "node1");
         clients[2] = new StandaloneManagementClient(node2Ip, node2Port, "node2");
         partitionHandlingController = new PartitionHandlingController(clients);
      }
   }

   /**
    * We use a cache with owners=1, otherwise the bigger partition can always handle all writes.
    */
   @Test
   public void testDenyReadWrites() throws InterruptedException {
      RemoteCacheManager cacheManager = ITestUtils.createCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(DENY_READ_WRITES_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache, 1);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache, 1);

      partitionCluster();

      RemoteCache<Object, Object> denyReadWritesCache1 = ITestUtils.createCacheManager(server1).getCache(DENY_READ_WRITES_CACHE);
      RemoteCache<Object, Object> denyReadWritesCache3 = ITestUtils.createCacheManager(server3).getCache(DENY_READ_WRITES_CACHE);

      denyReadWritesCache1.put(server1OwnedKey, "value");
      assertEquals("value", denyReadWritesCache1.get(server1OwnedKey));
      assertThrowsHotRodClientException(() -> denyReadWritesCache1.put(server3OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> denyReadWritesCache1.get(server3OwnedKey));

      denyReadWritesCache3.put(server3OwnedKey, "value");
      assertEquals("value", denyReadWritesCache3.get(server3OwnedKey));
      assertThrowsHotRodClientException(() -> denyReadWritesCache3.put(server1OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> denyReadWritesCache3.get(server1OwnedKey));

      healCluster();
   }

   @Test
   public void testAllowReads() throws InterruptedException {
      RemoteCacheManager cacheManager = ITestUtils.createCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(ALLOW_READS_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache, 1);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache, 1);

      partitionCluster();

      RemoteCache<Object, Object> allowReadsCache1 = ITestUtils.createCacheManager(server1).getCache(ALLOW_READS_CACHE);
      RemoteCache<Object, Object> allowReadsCache3 = ITestUtils.createCacheManager(server3).getCache(ALLOW_READS_CACHE);

      allowReadsCache1.put(server1OwnedKey, "value");
      assertEquals("value", allowReadsCache1.get(server1OwnedKey));
      assertThrowsHotRodClientException(() -> allowReadsCache1.put(server3OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> allowReadsCache1.get(server3OwnedKey));

      allowReadsCache3.put(server3OwnedKey, "value");
      assertEquals("value", allowReadsCache3.get(server3OwnedKey));
      assertThrowsHotRodClientException(() -> allowReadsCache3.put(server1OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> allowReadsCache3.get(server1OwnedKey));

      healCluster();
   }

   @Test
   public void testAllowReads2Owners() throws InterruptedException {
      RemoteCacheManager cacheManager = ITestUtils.createCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(ALLOW_READS_CACHE_2_OWNERS);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache, 2);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache, 2);

      cache.put(server3OwnedKey, "value");

      partitionCluster();

      RemoteCache<Object, Object> allowReadsCache1 = ITestUtils.createCacheManager(server1).getCache(ALLOW_READS_CACHE_2_OWNERS);
      RemoteCache<Object, Object> allowReadsCache3 = ITestUtils.createCacheManager(server3).getCache(ALLOW_READS_CACHE_2_OWNERS);

      allowReadsCache1.put(server1OwnedKey, "value");
      assertEquals("value", allowReadsCache1.get(server1OwnedKey));
      allowReadsCache1.put(server3OwnedKey, "value2");
      assertEquals("value2", allowReadsCache1.get(server3OwnedKey));

      assertThrowsHotRodClientException(() -> allowReadsCache3.put(server3OwnedKey, "value3"));
      assertEquals("value", allowReadsCache3.get(server3OwnedKey));

      healCluster();
   }

   @Test
   public void testAllowReadWrites() throws InterruptedException {
      RemoteCacheManager cacheManager = ITestUtils.createCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(ALLOW_READ_WRITES_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache, 1);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache, 1);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();

      RemoteCache<Object, Object> allowReadWritesCache1 = ITestUtils.createCacheManager(server1).getCache(ALLOW_READ_WRITES_CACHE);
      RemoteCache<Object, Object> allowReadWritesCache3 = ITestUtils.createCacheManager(server3).getCache(ALLOW_READ_WRITES_CACHE);

      assertEquals("value", allowReadWritesCache1.get(server1OwnedKey));
      allowReadWritesCache1.put(server1OwnedKey, "value1");
      assertEquals("value1", allowReadWritesCache1.get(server1OwnedKey));
      allowReadWritesCache1.put(server3OwnedKey, "value1");
      assertEquals("value1", allowReadWritesCache1.get(server3OwnedKey));

      assertEquals("value", allowReadWritesCache3.get(server3OwnedKey));
      allowReadWritesCache3.put(server3OwnedKey, "value2");
      assertEquals("value2", allowReadWritesCache3.get(server3OwnedKey));
      allowReadWritesCache3.put(server1OwnedKey, "value2");
      assertEquals("value2", allowReadWritesCache3.get(server1OwnedKey));

      healCluster();
   }

   private void partitionCluster() {
      partitionHandlingController.partitionCluster(new HashSet<>(Arrays.asList("node0", "node1")),
                                                   new HashSet<>(Arrays.asList("node2")));
   }

   private void healCluster() {
      partitionHandlingController.heal();
   }

   private void assertThrowsHotRodClientException(Runnable r) {
      try {
         r.run();
         fail();
      } catch (HotRodClientException hce){
         // OK
      }
   }

   private String getKeyOwnedByNode(String serverAddress, RemoteCacheManager rcm, RemoteCache cache, int owners) {
      SegmentConsistentHash hash = createHash(cache, NUM_SEGMENTS, owners);

      int segmentOwnedByNode = -1;
      for (Map.Entry<SocketAddress, Set<Integer>> socketAddressToSegments : cache.getCacheTopologyInfo().getSegmentsPerServer().entrySet()) {
         if (socketAddressToSegments.getKey().toString().equals(serverAddress)) {
            segmentOwnedByNode = socketAddressToSegments.getValue().stream().findFirst().get();
         }
      }

      int i = 0;
      while (hash.getSegment(marshall(rcm, "key" + i)) != segmentOwnedByNode) {
         i++;
      }
      return "key" + i;
   }

   private SegmentConsistentHash createHash(RemoteCache cache, int segments, int owners) {
      SocketAddress[][] mySegmentOwners = new SocketAddress[segments][owners];
      for (Map.Entry<SocketAddress, Set<Integer>> socketAddressSetEntry : cache.getCacheTopologyInfo().getSegmentsPerServer().entrySet()) {
         SocketAddress server = socketAddressSetEntry.getKey();
         for (Integer integer : socketAddressSetEntry.getValue()) {
            int emptyIndex = 0;
            while (mySegmentOwners[integer][emptyIndex] != null) {
               emptyIndex++;
            }
            mySegmentOwners[integer][emptyIndex] = server;
         }
      }
      SegmentConsistentHash result = new SegmentConsistentHash();
      result.init(mySegmentOwners, segments);
      return result;
   }

   private byte[] marshall(RemoteCacheManager rcm, Object key) {
      try {
         return rcm.getMarshaller().objectToByteBuffer(key);
      } catch (Exception e) {
         throw new RuntimeException("Marshalling a key failed.", e);
      }
   }

}