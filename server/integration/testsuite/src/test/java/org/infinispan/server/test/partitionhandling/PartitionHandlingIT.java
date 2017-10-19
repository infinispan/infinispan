package org.infinispan.server.test.partitionhandling;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.JGroupsProbeClient;
import org.infinispan.server.test.util.PartitionHandlingController;
import org.infinispan.server.test.util.StandaloneManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.infinispan.server.test.util.ITestUtils.eventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

   final static String DENY_READ_WRITES_CACHE = "denyreadwrites";
   final static String ALLOW_READS_CACHE = "allowreads";
   final static String ALLOW_READS_CACHE_2_OWNERS = "allowreads_2owners";
   final static String ALLOW_READ_WRITES_CACHE = "allowreadwrites";
   final static String REMOVE_ALL_CACHE = "removeall";
   final static String PREFERRED_ALWAYS_CACHE = "preferredalways";
   final static String PREFERRED_NON_NULL_CACHE = "preferrednonnull";
   final static String CUSTOM_ENTRY_MERGE_POLICY_CACHE = "customentrymergepolicy";

   private PartitionHandlingController partitionHandlingController;
   private boolean healed = true; // in case a test fails before healing the cluster

   @InfinispanResource(CONTAINER1)
   private RemoteInfinispanServer server1;
   private String server1Address;

   @InfinispanResource(CONTAINER2)
   private RemoteInfinispanServer server2;

   @InfinispanResource(CONTAINER3)
   private RemoteInfinispanServer server3;
   private String server3Address;

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

         StandaloneManagementClient[] managementClients = new StandaloneManagementClient[3];
         managementClients[0] = new StandaloneManagementClient(node0Ip, node0Port, "node0");
         managementClients[1] = new StandaloneManagementClient(node1Ip, node1Port, "node1");
         managementClients[2] = new StandaloneManagementClient(node2Ip, node2Port, "node2");
         Map<String, JGroupsProbeClient> probeClients = new HashMap<>();
         probeClients.put("node0", new JGroupsProbeClient("224.0.75.75", 7500));
         probeClients.put("node1", new JGroupsProbeClient("224.0.75.76", 7500));
         probeClients.put("node2", new JGroupsProbeClient("224.0.75.77", 7500));
         partitionHandlingController = new PartitionHandlingController(managementClients, probeClients);
      }
   }

   @BeforeClass
   public static void before() throws Exception {
      JavaArchive deployedCacheStore = ShrinkWrap.create(JavaArchive.class);
      deployedCacheStore.addPackage(CustomEntryMergePolicy.class.getPackage());
      deployedCacheStore.addAsServiceProvider(EntryMergePolicy.class, CustomEntryMergePolicy.class);

      deployedCacheStore.as(ZipExporter.class).exportTo(
            new File(System.getProperty("server1.dist"), "/standalone/deployments/custom-entry-merge-policy.jar"), true);
      deployedCacheStore.as(ZipExporter.class).exportTo(
            new File(System.getProperty("server2.dist"), "/standalone/deployments/custom-entry-merge-policy.jar"), true);
      deployedCacheStore.as(ZipExporter.class).exportTo(
            new File(System.getProperty("server3.dist"), "/standalone/deployments/custom-entry-merge-policy.jar"), true);
   }

   @After
   public void after() {
      if (!healed) {
         healCluster();
      }
   }

   // We use a cache with owners=1, otherwise the bigger partition can always handle all writes.
   @Test
   public void testDenyReadWrites() throws InterruptedException {
      testCommonLogic(DENY_READ_WRITES_CACHE);
   }

   @Test
   public void testAllowReads() throws InterruptedException {
      testCommonLogic(ALLOW_READS_CACHE);
   }

   // allowReads and denyReadWrites behave the same from the client's view when owners=1
   public void testCommonLogic(String cacheName) throws InterruptedException {
      eventually(() -> assertNoRebalance(cacheName, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(cacheName);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      partitionCluster();
      eventually(() -> assertNoRebalance(cacheName, server1, server2), 10000);
      eventually(() -> assertNoRebalance(cacheName, server3), 10000);

      RemoteCache<Object, Object> cache1 = ITestUtils.createInternalCacheManager(server1).getCache(cacheName);
      RemoteCache<Object, Object> cache3 = ITestUtils.createInternalCacheManager(server3).getCache(cacheName);

      cache1.put(server1OwnedKey, "value");
      assertEquals("value", cache1.get(server1OwnedKey));
      assertThrowsHotRodClientException(() -> cache1.put(server3OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> cache1.get(server3OwnedKey));

      cache3.put(server3OwnedKey, "value");
      assertEquals("value", cache3.get(server3OwnedKey));
      assertThrowsHotRodClientException(() -> cache3.put(server1OwnedKey, "value"));
      assertThrowsHotRodClientException(() -> cache3.get(server1OwnedKey));

      healCluster();
   }

   @Test
   public void testAllowReads2Owners() throws InterruptedException {
      eventually(() -> assertNoRebalance(ALLOW_READS_CACHE_2_OWNERS, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(ALLOW_READS_CACHE_2_OWNERS);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(ALLOW_READS_CACHE_2_OWNERS, server1, server2), 10000);
      eventually(() -> assertNoRebalance(ALLOW_READS_CACHE_2_OWNERS, server3), 10000);

      RemoteCache<Object, Object> allowReadsCache1 = ITestUtils.createInternalCacheManager(server1).getCache(ALLOW_READS_CACHE_2_OWNERS);
      RemoteCache<Object, Object> allowReadsCache3 = ITestUtils.createInternalCacheManager(server3).getCache(ALLOW_READS_CACHE_2_OWNERS);

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
      eventually(() -> assertNoRebalance(ALLOW_READ_WRITES_CACHE, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(ALLOW_READ_WRITES_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(ALLOW_READ_WRITES_CACHE, server1, server2), 10000);
      eventually(() -> assertNoRebalance(ALLOW_READ_WRITES_CACHE, server3), 10000);

      RemoteCache<Object, Object> allowReadWritesCache1 = ITestUtils.createInternalCacheManager(server1).getCache(ALLOW_READ_WRITES_CACHE);
      RemoteCache<Object, Object> allowReadWritesCache3 = ITestUtils.createInternalCacheManager(server3).getCache(ALLOW_READ_WRITES_CACHE);

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

   @Test
   public void testPreferredAlwaysEntryMergePolicy() throws InterruptedException {
      eventually(() -> assertNoRebalance(PREFERRED_ALWAYS_CACHE, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(PREFERRED_ALWAYS_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(PREFERRED_ALWAYS_CACHE, server1, server2), 10000);
      eventually(() -> assertNoRebalance(PREFERRED_ALWAYS_CACHE, server3), 10000);

      RemoteCache<Object, Object> preferredAlwaysCache1 = ITestUtils.createInternalCacheManager(server1).getCache(PREFERRED_ALWAYS_CACHE);
      RemoteCache<Object, Object> preferredAlwaysCache3 = ITestUtils.createInternalCacheManager(server3).getCache(PREFERRED_ALWAYS_CACHE);

      preferredAlwaysCache1.put(server1OwnedKey, "value1");
      preferredAlwaysCache1.put(server3OwnedKey, "value1");
      preferredAlwaysCache3.put(server1OwnedKey, "value3");
      preferredAlwaysCache3.put(server3OwnedKey, "value3");

      healCluster();
      eventually(() -> assertNoRebalance(PREFERRED_ALWAYS_CACHE, server1, server2, server3), 10000);

      assertEquals("value1", cache.get(server1OwnedKey));
      assertEquals("value1", cache.get(server3OwnedKey));
   }

   @Test
   public void testPreferredNonNullEntryMergePolicy() throws InterruptedException {
      eventually(() -> assertNoRebalance(PREFERRED_NON_NULL_CACHE, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(PREFERRED_NON_NULL_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(PREFERRED_NON_NULL_CACHE, server1, server2), 10000);
      eventually(() -> assertNoRebalance(PREFERRED_NON_NULL_CACHE, server3), 10000);

      RemoteCache<Object, Object> preferredNonNullCache1 = ITestUtils.createInternalCacheManager(server1).getCache(PREFERRED_NON_NULL_CACHE);
      RemoteCache<Object, Object> preferredNonNullCache3 = ITestUtils.createInternalCacheManager(server3).getCache(PREFERRED_NON_NULL_CACHE);

      preferredNonNullCache1.put(server1OwnedKey, "value1");
      preferredNonNullCache1.remove(server3OwnedKey);
      preferredNonNullCache3.put(server1OwnedKey, "value3");
      preferredNonNullCache3.put(server3OwnedKey, "value3");

      healCluster();
      eventually(() -> assertNoRebalance(PREFERRED_NON_NULL_CACHE, server1, server2, server3), 10000);

      assertEquals("value1", cache.get(server1OwnedKey));
      assertEquals("value3", cache.get(server3OwnedKey));
   }

   @Test
   public void testRemoveAllEntryMergePolicy() throws InterruptedException {
      eventually(() -> assertNoRebalance(REMOVE_ALL_CACHE, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(REMOVE_ALL_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(REMOVE_ALL_CACHE, server1, server2), 10000);
      eventually(() -> assertNoRebalance(REMOVE_ALL_CACHE, server3), 10000);

      RemoteCache<Object, Object> removeAllCache1 = ITestUtils.createInternalCacheManager(server1).getCache(REMOVE_ALL_CACHE);
      RemoteCache<Object, Object> removeAllCache3 = ITestUtils.createInternalCacheManager(server3).getCache(REMOVE_ALL_CACHE);

      removeAllCache1.put(server1OwnedKey, "value1");
      removeAllCache1.put(server3OwnedKey, "value1");
      removeAllCache3.put(server1OwnedKey, "value3");
      removeAllCache3.put(server3OwnedKey, "value3");

      healCluster();
      eventually(() -> assertNoRebalance(REMOVE_ALL_CACHE, server1, server2, server3), 10000);

      assertNull(cache.get(server1OwnedKey));
      assertNull(cache.get(server3OwnedKey));
   }

   @Test
   public void testCustomEntryMergePolicy() throws InterruptedException {
      eventually(() -> assertNoRebalance(CUSTOM_ENTRY_MERGE_POLICY_CACHE, server1, server2, server3), 10000);
      RemoteCacheManager cacheManager = ITestUtils.createInternalCacheManager(server2);
      RemoteCache<Object, Object> cache = cacheManager.getCache(CUSTOM_ENTRY_MERGE_POLICY_CACHE);

      String server1OwnedKey = getKeyOwnedByNode(server1Address, cacheManager, cache);
      String server3OwnedKey = getKeyOwnedByNode(server3Address, cacheManager, cache);

      cache.put(server1OwnedKey, "value");
      cache.put(server3OwnedKey, "value");

      partitionCluster();
      eventually(() -> assertNoRebalance(CUSTOM_ENTRY_MERGE_POLICY_CACHE, server1, server2), 10000);
      eventually(() -> assertNoRebalance(CUSTOM_ENTRY_MERGE_POLICY_CACHE, server3), 10000);

      RemoteCache<Object, Object> customEntryMergePolicyCache1 = ITestUtils.createInternalCacheManager(server1).getCache(CUSTOM_ENTRY_MERGE_POLICY_CACHE);
      RemoteCache<Object, Object> customEntryMergePolicyCache3 = ITestUtils.createInternalCacheManager(server3).getCache(CUSTOM_ENTRY_MERGE_POLICY_CACHE);

      customEntryMergePolicyCache1.put(server1OwnedKey, "value1");
      customEntryMergePolicyCache1.put(server3OwnedKey, "value1");
      customEntryMergePolicyCache3.put(server1OwnedKey, "value3");
      customEntryMergePolicyCache3.put(server3OwnedKey, "value3");

      healCluster();
      eventually(() -> assertNoRebalance(CUSTOM_ENTRY_MERGE_POLICY_CACHE, server1, server2, server3), 10000);

      assertEquals("customValue", cache.get(server1OwnedKey));
      assertEquals("customValue", cache.get(server3OwnedKey));
   }

   /**
    * This check exists because the rebalance happens after the view change, and if any test runs in this window the getKeyOwnedByNode
    * method may end in an infinite loop. The rebalance status could be checked in PartitionHandlingController using creeper, but for an
    * unknown reason that doesn't work, so this is sort of a workaround.
    */
   private boolean assertNoRebalance(String cacheName, RemoteInfinispanServer... servers) {
      return Arrays.stream(servers).allMatch(
            server -> ITestUtils.createInternalCacheManager(server).getCache(cacheName).getCacheTopologyInfo().getSegmentsPerServer().size() == servers.length);
   }

   private void partitionCluster() {
      partitionHandlingController.partitionCluster(new HashSet<>(Arrays.asList("node0", "node1")),
                                                   new HashSet<>(Arrays.asList("node2")));
      healed = false;
   }

   private void healCluster() {
      partitionHandlingController.heal();
      healed = true;
   }

   private void assertThrowsHotRodClientException(Runnable r) {
      try {
         r.run();
         fail();
      } catch (HotRodClientException hce){
         // OK
      }
   }

   private String getKeyOwnedByNode(String serverAddress, RemoteCacheManager rcm, RemoteCache cache) {
      SegmentConsistentHash hash = createHash(cache);

      int segmentOwnedByNode = -1;
      for (Map.Entry<SocketAddress, Set<Integer>> socketAddressToSegments : hash.getSegmentsByServer().entrySet()) {
         if (socketAddressToSegments.getKey().toString().equals(serverAddress)) {
            segmentOwnedByNode = socketAddressToSegments.getValue().stream().findFirst().get();
         }
      }

      int i = 0;
      while (hash.getSegment(marshall(rcm, "key" + i)) != segmentOwnedByNode) {
         i++;
         if (i > 10000) { // don't loop forever
            throw new IllegalStateException("Server " + serverAddress + " is not an owner of any key.");
         }
      }
      return "key" + i;
   }

   private SegmentConsistentHash createHash(RemoteCache cache) {
      TransportFactory transportFactory = ((InternalRemoteCacheManager) cache.getRemoteCacheManager()).getTransportFactory();
      return (SegmentConsistentHash) transportFactory.getConsistentHash(RemoteCacheManager.cacheNameBytes(cache.getName()));
   }

   private byte[] marshall(RemoteCacheManager rcm, Object key) {
      try {
         return rcm.getMarshaller().objectToByteBuffer(key);
      } catch (Exception e) {
         throw new RuntimeException("Marshalling a key failed.", e);
      }
   }

}