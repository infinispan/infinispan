package org.infinispan.server.hotrod.test;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.server.hotrod.OperationStatus.KeyDoesNotExist;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.ServerAddress;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.SingleByteFrameDecoderChannelInitializer;
import org.infinispan.server.hotrod.transport.TimeoutEnabledChannelInitializer;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.KeyValuePair;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.util.concurrent.Future;

/**
 * Test utils for Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class HotRodTestingUtil {
   private HotRodTestingUtil() {
   }

   private static final Log log = LogFactory.getLog(HotRodTestingUtil.class, Log.class);
   private static final UniquePortThreadLocal uptl = new UniquePortThreadLocal();

   public static final byte EXPECTED_HASH_FUNCTION_VERSION = 2;

   public static String host() {
      return "127.0.0.1";
   }

   public static int serverPort() {
      return uptl.get();
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager) {
      return startHotRodServer(manager, serverPort());
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, String defaultCacheName) {
      return startHotRodServer(manager, serverPort(), 0, host(), serverPort(), 0, defaultCacheName);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, String proxyHost, int proxyPort) {
      return startHotRodServer(manager, serverPort(), 0, proxyHost, proxyPort);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port) {
      return startHotRodServer(manager, port, 0);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, String proxyHost, int proxyPort) {
      return startHotRodServer(manager, port, 0, proxyHost, proxyPort);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout) {
      return startHotRodServer(manager, port, idleTimeout, host(), port);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout, String proxyHost, int proxyPort) {
      return startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort, -1);
   }

   public static HotRodServer startHotRodServerWithDelay(EmbeddedCacheManager manager, int port, long delay) {
      return startHotRodServer(manager, port, 0, host(), port, delay);
   }

   public static HotRodServer startHotRodServerWithoutTransport(String... definedCaches) {
      return startHotRodServerWithoutTransport(new HotRodServerConfigurationBuilder(), definedCaches);
   }

   public static HotRodServer startHotRodServerWithoutTransport(HotRodServerConfigurationBuilder builder, String... definedCaches) {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();

      Configuration cacheConfiguration = new ConfigurationBuilder()
            .encoding().key().mediaType(APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(APPLICATION_OBJECT_TYPE)
            .build();

      builder.startTransport(false);

      DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfiguration.build(), cacheConfiguration);
      for (String cache : definedCaches) {
         cacheManager.defineConfiguration(cache, cacheConfiguration);
      }

      return startHotRodServer(cacheManager, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout,
                                                String proxyHost, int proxyPort, long delay, String defaultCacheName) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.proxyHost(proxyHost).proxyPort(proxyPort).idleTimeout(idleTimeout).defaultCacheName(defaultCacheName);
      return startHotRodServer(manager, port, delay, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout,
                                                String proxyHost, int proxyPort, long delay) {
      return startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort, delay, BasicCacheContainer.DEFAULT_CACHE_NAME);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, HotRodServerConfigurationBuilder builder) {
      return startHotRodServer(manager, host(), port, 0L, false, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, HotRodServerConfigurationBuilder builder) {
      return startHotRodServer(manager, serverPort(), 0L, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, long delay, HotRodServerConfigurationBuilder builder) {
      return startHotRodServer(manager, host(), port, delay, false, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, String host, int port, long delay, HotRodServerConfigurationBuilder builder) {
      return startHotRodServer(manager, host, port, delay, false, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, String host, int port, long delay, boolean perf, HotRodServerConfigurationBuilder builder) {
      log.infof("Start server in port %d", port);
      HotRodServer server = new HotRodServer() {
         @Override
         protected ConfigurationBuilder createTopologyCacheConfig(long distSyncTimeout) {
            if (delay > 0)
               try {
                  Thread.sleep(delay);
               } catch (InterruptedException e) {
                  throw new CacheException(e);
               }

            return super.createTopologyCacheConfig(distSyncTimeout);
         }

         @Override
         public ChannelInitializer<Channel> getInitializer() {
            // Pass by name since we have circular dependency
            if (perf) {
               if (configuration.idleTimeout() > 0)
                  return new NettyInitializers(
                        new NettyChannelInitializer<>(this, transport, getEncoder(), getDecoder()),
                        new TimeoutEnabledChannelInitializer<>(this));
               else // Idle timeout logic is disabled with -1 or 0 values
                  return new NettyInitializers(new NettyChannelInitializer<>(this, transport, getEncoder(), getDecoder()));
            } else {
               if (configuration.idleTimeout() > 0)
                  return new NettyInitializers(
                        new NettyChannelInitializer<>(this, transport, getEncoder(), getDecoder()),
                        new TimeoutEnabledChannelInitializer<>(this), new SingleByteFrameDecoderChannelInitializer());
               else // Idle timeout logic is disabled with -1 or 0 values
                  return new NettyInitializers(
                        new NettyChannelInitializer<>(this, transport, getEncoder(), getDecoder()),
                        new SingleByteFrameDecoderChannelInitializer());
            }
         }
      };
      String shortTestName = TestResourceTracker.getCurrentTestShortName();
      if (!builder.name().contains(shortTestName)) {
         // Only set the name once if HotRodClientTestingUtil.startHotRodServer() retries
         builder.name(shortTestName + builder.name());
      }
      builder.host(host).port(port);
      builder.ioThreads(3);
      server.start(builder.build(), manager);

      return server;
   }

   public static HotRodServerConfigurationBuilder getDefaultHotRodConfiguration() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      int port = serverPort();
      builder.host(host()).port(port).proxyHost(host()).proxyPort(port);
      return builder;
   }

   public static Iterator<NetworkInterface> findNetworkInterfaces(boolean loopback) {
      try {
         List<NetworkInterface> matchingInterfaces = new ArrayList<>();
         Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
         while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isUp() && ni.isLoopback() == loopback && ni.getInetAddresses().hasMoreElements()) {
               matchingInterfaces.add(ni);
            }
         }
         return matchingInterfaces.iterator();
      } catch (SocketException e) {
         throw new CacheException(e);
      }
   }

   public static byte[] k(Method m, String prefix) {
      byte[] bytes = (prefix + m.getName()).getBytes();
      log.tracef("String %s is converted to %s bytes", prefix + m.getName(), Util.printArray(bytes, true));
      return bytes;
   }

   public static byte[] v(Method m, String prefix) {
      return k(m, prefix);
   }

   public static byte[] k(Method m) {
      return k(m, "k-");
   }

   public static byte[] v(Method m) {
      return v(m, "v-");
   }

   public static boolean assertStatus(TestResponse resp, OperationStatus expected) {
      OperationStatus status = resp.getStatus();
      boolean isSuccess = status == expected;
      if (resp instanceof TestErrorResponse) {
         assertTrue(String.format("Status should have been '%s' but instead was: '%s', and the error message was: %s",
               expected, status, ((TestErrorResponse) resp).msg), isSuccess);
      } else {
         assertTrue(String.format("Status should have been '%s' but instead was: '%s'", expected, status), isSuccess);
      }
      return isSuccess;
   }

   public static boolean assertSuccess(TestGetResponse resp, byte[] expected) {
      assertStatus(resp, Success);
      boolean isArrayEquals = Arrays.equals(expected, resp.data.get());
      assertTrue("Retrieved data should have contained " + Util.printArray(expected, true) + " (" + new String(expected)
            + "), but instead we received " + Util.printArray(resp.data.get(), true) + " (" +  new String(resp.data.get()) +")", isArrayEquals);
      return isArrayEquals;
   }

   public static void assertByteArrayEquals(byte[] expected, byte[] actual) {
      boolean isArrayEquals = Arrays.equals(expected, actual);
      assertTrue("Retrieved data should have contained " + Util.printArray(expected, true) + " (" + new String(expected)
            + "), but instead we received " + Util.printArray(actual, true) + " (" +  new String(actual) +")", isArrayEquals);
   }

   public static boolean assertSuccess(TestGetWithVersionResponse resp, byte[] expected, int expectedVersion) {
      assertTrue(resp.getVersion() != expectedVersion);
      return assertSuccess(resp, expected);
   }

   public static boolean assertSuccess(TestGetWithMetadataResponse resp, byte[] expected, int expectedLifespan, int expectedMaxIdle) {
      assertEquals(resp.lifespan, expectedLifespan);
      assertEquals(resp.maxIdle, expectedMaxIdle);
      return assertSuccess(resp, expected);
   }

   public static boolean assertKeyDoesNotExist(TestGetResponse resp) {
      OperationStatus status = resp.getStatus();
      assertTrue("Status should have been 'KeyDoesNotExist' but instead was: " + status, status == KeyDoesNotExist);
      assertEquals(resp.data, Optional.empty());
      return status == KeyDoesNotExist;
   }

   public static void assertTopologyReceived(AbstractTestTopologyAwareResponse resp, List<HotRodServer> servers,
                                             int expectedTopologyId ) {
      assertEquals(resp.topologyId, expectedTopologyId);
      if (resp instanceof TestHashDistAware10Response) {
         TestHashDistAware10Response h10 = (TestHashDistAware10Response) resp;
         assertEquals(new HashSet<>(h10.members), servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
      } else if (resp instanceof TestHashDistAware11Response) {
         TestHashDistAware11Response h11 = (TestHashDistAware11Response) resp;
         assertEquals(new HashSet<>(h11.members), servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
      } else if (resp instanceof TestTopologyAwareResponse) {
         TestTopologyAwareResponse t = (TestTopologyAwareResponse) resp;
         assertEquals(new HashSet<>(t.members), servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
      } else {
         throw new IllegalArgumentException("Unsupported response!");
      }
   }

   public static void assertHashTopology20Received(AbstractTestTopologyAwareResponse topoResp,
                                                   List<HotRodServer> servers, String cacheName, int expectedTopologyId) {
      TestHashDistAware20Response hashTopologyResp = (TestHashDistAware20Response) topoResp;
      assertEquals(expectedTopologyId, hashTopologyResp.topologyId);
      Set<ServerAddress> serverAddresses = servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet());
      assertEquals(new HashSet<>(hashTopologyResp.members), serverAddresses);
      assertEquals(hashTopologyResp.hashFunction, 3);
      // Assert segments
      Cache cache = servers.get(0).getCacheManager().getCache(cacheName);
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      ConsistentHash ch = distributionManager.getCacheTopology().getCurrentCH();
      int numSegments = ch.getNumSegments();
      int numOwners = ch.getNumOwners();
      assertEquals(hashTopologyResp.segments.size(), numSegments);
      for (int i = 0; i < numSegments; ++i) {
         List<Address> segment = ch.locateOwnersForSegment(i);
         Iterable<ServerAddress> members = hashTopologyResp.segments.get(i);
         assertEquals(numOwners, segment.size());
         int count = 0;
         for (ServerAddress member : members) {
            count++;
            assertTrue(serverAddresses.contains(member));
         }
         assertEquals(numOwners, count);
      }
   }

   public static void assertHashTopology10Received(AbstractTestTopologyAwareResponse topoResp, List<HotRodServer> servers,
                                                   String cacheName, int expectedTopologyId) {
      assertHashTopology10Received(topoResp, servers, cacheName, 2,
            EXPECTED_HASH_FUNCTION_VERSION, Integer.MAX_VALUE, expectedTopologyId);
   }

   public static void assertNoHashTopologyReceived(AbstractTestTopologyAwareResponse topoResp, List<HotRodServer> servers,
                                                   String cacheName, int expectedTopologyId) {
      if (topoResp instanceof TestHashDistAware10Response) {
         assertHashTopology10Received(topoResp, servers, cacheName, 0, 0, 0, expectedTopologyId);
      } else if (topoResp instanceof TestHashDistAware20Response) {
         TestHashDistAware20Response t = (TestHashDistAware20Response) topoResp;
         assertEquals(t.topologyId, expectedTopologyId);
         assertEquals(new HashSet<>(t.members),
                      servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
         assertEquals(t.hashFunction, 0);
         assertEquals(t.segments.size(), 0);
      } else {
         throw new IllegalArgumentException("Unsupported response!");
      }
   }

   public static void assertHashTopology10Received(AbstractTestTopologyAwareResponse topoResp,
                                                   List<HotRodServer> servers, String cacheName,
                                                   int expectedNumOwners, int expectedHashFct, int expectedHashSpace,
                                                   int expectedTopologyId) {
      TestHashDistAware10Response hashTopologyResp = (TestHashDistAware10Response) topoResp;
      assertEquals(hashTopologyResp.topologyId, expectedTopologyId);
      assertEquals(new HashSet<>(hashTopologyResp.members),
                   servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
      assertEquals(hashTopologyResp.numOwners, expectedNumOwners);
      assertEquals(hashTopologyResp.hashFunction, expectedHashFct);
      assertEquals(hashTopologyResp.hashSpace, expectedHashSpace);
      if (expectedNumOwners != 0) // Hash ids worth comparing
         assertHashIds(hashTopologyResp.hashIds, servers, cacheName);
   }

   public static void assertHashTopologyReceived(AbstractTestTopologyAwareResponse topoResp,
                                                 List<HotRodServer> servers, String cacheName,
                                                 int expectedNumOwners, int expectedVirtualNodes,
                                                 int expectedTopologyId) {
      TestHashDistAware11Response hashTopologyResp = (TestHashDistAware11Response) topoResp;
      assertEquals(hashTopologyResp.topologyId, expectedTopologyId);
      assertEquals(new HashSet<>(hashTopologyResp.members),
                   servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet()));
      assertEquals(hashTopologyResp.numOwners, expectedNumOwners);
      assertEquals(hashTopologyResp.hashFunction,
            expectedNumOwners != 0 ? EXPECTED_HASH_FUNCTION_VERSION : 0);
      assertEquals(hashTopologyResp.hashSpace,
            expectedNumOwners != 0 ? Integer.MAX_VALUE : 0);
      assertEquals(hashTopologyResp.numVirtualNodes, expectedVirtualNodes);
   }

   public static void assertHashIds(Map<ServerAddress, List<Integer>> hashIds, List<HotRodServer> servers, String cacheName) {
      Cache cache = servers.get(0).getCacheManager().getCache(cacheName);
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      ConsistentHash consistentHash = distributionManager.getCacheTopology().getCurrentCH();
      int numSegments = consistentHash.getNumSegments();
      int numOwners = consistentHash.getNumOwners();
      assertEquals(hashIds.size(), servers.size());

      int segmentSize = (int) Math.ceil((double) Integer.MAX_VALUE / numSegments);
      Map<Integer, ServerAddress>[] owners = new Map[numSegments];

      for (Map.Entry<ServerAddress, List<Integer>> entry : hashIds.entrySet()) {
         ServerAddress serverAddress = entry.getKey();
         List<Integer> serverHashIds = entry.getValue();
         for (Integer hashId : serverHashIds) {
            int segmentIdx = (hashId / segmentSize + numSegments - 1) % numSegments;
            int ownerIdx = hashId % segmentSize;
            if (owners[segmentIdx] == null) {
               owners[segmentIdx] = new HashMap<>();
            }
            owners[segmentIdx].put(ownerIdx, serverAddress);
         }
      }

      for (int i = 0; i < numSegments; ++i) {
         List<ServerAddress> segmentOwners = owners[i].entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
               .map(Map.Entry::getValue).collect(Collectors.toList());
         assertEquals(segmentOwners.size(), numOwners);
         List<ServerAddress> chOwners = consistentHash.locateOwnersForSegment(i).stream()
               .map(a -> clusterAddressToServerAddress(servers, a)).collect(Collectors.toList());
         assertEquals(segmentOwners, chOwners);
      }
   }

   public static void assertReplicatedHashIds(Map<ServerAddress, List<Integer>> hashIds, List<HotRodServer> servers,
                                              String cacheName) {
      Cache cache = servers.get(0).getCacheManager().getCache(cacheName);
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      ConsistentHash consistentHash = distributionManager.getCacheTopology().getCurrentCH();
      int numSegments = consistentHash.getNumSegments();
      int numOwners = consistentHash.getNumOwners();

      // replicated responses have just one segment, and each server should have only one hash id: 0
      assertEquals(hashIds.size(), servers.size());
      assertEquals(numSegments, 1);

      for (Map.Entry<ServerAddress, List<Integer>> entry : hashIds.entrySet()) {
         List<Integer> serverHashIds = entry.getValue();
         assertEquals(serverHashIds.size(), 1);
         assertEquals(serverHashIds.get(0).intValue(), 0);
      }
   }

   private static ServerAddress clusterAddressToServerAddress(List<HotRodServer> servers, Address clusterAddress ) {
      Optional<HotRodServer> match = servers.stream().filter(a -> a.getCacheManager().getAddress().equals(clusterAddress)).findFirst();
      return match.get().getAddress();
   }

   public static int getServerTopologyId(EmbeddedCacheManager cm, String cacheName) {
      return cm.getCache(cacheName).getAdvancedCache().getRpcManager().getTopologyId();
   }

   public static Future<?> killClient(HotRodClient client) {
      try {
         if (client != null) return client.stop();
      }
      catch (Throwable t) {
         log.error("Error stopping client", t);
      }
      return null;
   }

   public static ConfigurationBuilder hotRodCacheConfiguration() {
      return new ConfigurationBuilder();
   }

   public static ConfigurationBuilder hotRodCacheConfiguration(ConfigurationBuilder builder) {
      return builder;
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, byte[] key, byte[] expectedValue) {
      return assertHotRodEquals(cm, cm.getCache(), key, expectedValue);
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String cacheName,
                                                       byte[] key, byte[] expectedValue) {
      return assertHotRodEquals(cm, cm.getCache(cacheName), key, expectedValue);
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String key, String expectedValue) {
      return assertHotRodEquals(cm, cm.getCache(), marshall(key), marshall(expectedValue));
   }


   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String cacheName,
                                                       String key, String expectedValue) {
      return assertHotRodEquals(cm, cm.getCache(cacheName), marshall(key), marshall(expectedValue));
   }

   private static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, Cache<byte[], byte[]> cache,
                                                        byte[] key, byte[] expectedValue) {
      CacheEntry<byte[], byte[]> entry = cache.getAdvancedCache().getCacheEntry(key);
      // Assert based on passed parameters
      if (expectedValue == null) {
         assertNull(entry);
      } else {
         byte[] value = entry.getValue();
         assertEquals(expectedValue, value);
      }

      return entry;
   }

   public static byte[] marshall(Object obj) {
      try {
         return obj == null ? null : new JBossMarshaller().objectToByteBuffer(obj, 64);
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }


   public static <T> T unmarshall(byte[] key) {
      try {
         return (T) new JBossMarshaller().objectFromByteBuffer(key);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   public static void withClientListener(HotRodClient client, TestClientListener listener,
                                         Optional<KeyValuePair<String, List<byte[]>>> filterFactory,
                                         Optional<KeyValuePair<String, List<byte[]>>> converterFactory, Runnable fn) {
      withClientListener(client, listener, filterFactory, converterFactory, false, true, fn);
   }

   public static void withClientListener(HotRodClient client, TestClientListener listener,
                                         Optional<KeyValuePair<String, List<byte[]>>> filterFactory,
                                         Optional<KeyValuePair<String, List<byte[]>>> converterFactory, boolean includeState, boolean useRawData,
                                         Runnable fn) {
      assertStatus(client.addClientListener(listener, includeState, filterFactory == null ? Optional.empty() : filterFactory,
            converterFactory == null ? Optional.empty() : converterFactory, useRawData), Success);
      try {
         fn.run();
      } finally {
         assertStatus(client.removeClientListener(listener.getId()), Success);
      }
   }

   @Listener
   public static class AddressRemovalListener {
      private final CountDownLatch latch;

      private AddressRemovalListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @CacheEntryRemoved
      public void addressRemoved(CacheEntryRemovedEvent<Address, ServerAddress> event) {
         if (!event.isPre()) // Only count down latch after address has been removed
            latch.countDown();
      }
   }

   static final AtomicInteger uniqueAddr = new AtomicInteger(12411);

   static class UniquePortThreadLocal extends ThreadLocal<Integer> {
      @Override
      protected Integer initialValue() {
         log.debugf("Before incrementing, server port is: %d", uniqueAddr.get());
         int port = uniqueAddr.getAndAdd(110);
         log.debugf("For next thread, server port will be: %d", uniqueAddr.get());
         return port;
      }
   }
}
