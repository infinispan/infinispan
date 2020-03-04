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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
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
import org.infinispan.util.KeyValuePair;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

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

      DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfiguration.build());
      for (String cache : definedCaches) {
         cacheManager.defineConfiguration(cache, cacheConfiguration);
      }

      return startHotRodServer(cacheManager, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout,
                                                String proxyHost, int proxyPort, long delay, String defaultCacheName) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.proxyHost(proxyHost).proxyPort(proxyPort).idleTimeout(idleTimeout);
      if (defaultCacheName != null) {
         builder.defaultCacheName(defaultCacheName);
      }
      return startHotRodServer(manager, port, delay, builder);
   }

   public static HotRodServer startHotRodServer(EmbeddedCacheManager manager, int port, int idleTimeout,
                                                String proxyHost, int proxyPort, long delay) {
      return startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort, delay, null);
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
      try {
         server.start(builder.build(), manager);
         return server;
      } catch (Throwable t) {
         server.stop();
         throw t;
      }
   }

   public static HotRodServerConfigurationBuilder getDefaultHotRodConfiguration() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      int port = serverPort();
      builder.host(host()).port(port).proxyHost(host()).proxyPort(port);
      return builder;
   }

   public static List<NetworkInterface> findNetworkInterfaces(boolean loopback) {
      try {
         List<NetworkInterface> matchingInterfaces = new ArrayList<>();
         Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
         while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isUp() && ni.isLoopback() == loopback && ni.getInetAddresses().hasMoreElements()) {
               matchingInterfaces.add(ni);
            }
         }
         return matchingInterfaces;
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
            + "), but instead we received " + Util.printArray(resp.data.get(), true) + " (" + new String(resp.data.get()) + ")", isArrayEquals);
      return isArrayEquals;
   }

   public static void assertByteArrayEquals(byte[] expected, byte[] actual) {
      boolean isArrayEquals = Arrays.equals(expected, actual);
      assertTrue("Retrieved data should have contained " + Util.printArray(expected, true) + " (" + new String(expected)
            + "), but instead we received " + Util.printArray(actual, true) + " (" + new String(actual) + ")", isArrayEquals);
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
                                             int expectedTopologyId) {
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
      assertEquals(hashTopologyResp.members.size(), servers.size());
      Set<ServerAddress> serverAddresses = servers.stream().map(HotRodServer::getAddress).collect(Collectors.toSet());
      hashTopologyResp.members.forEach(member -> assertTrue(serverAddresses.contains(member)));
      assertEquals(hashTopologyResp.hashFunction, 3);
      // Assert segments
      Cache cache = servers.get(0).getCacheManager().getCache(cacheName);
      LocalizedCacheTopology cacheTopology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
      assertEquals(cacheTopology.getActualMembers().size(), servers.size());
      ConsistentHash ch = cacheTopology.getCurrentCH();
      int numSegments = ch.getNumSegments();
      int numOwners = cache.getCacheConfiguration().clustering().hash().numOwners();
      assertEquals(hashTopologyResp.segments.size(), numSegments);
      for (int i = 0; i < numSegments; ++i) {
         List<Address> segment = ch.locateOwnersForSegment(i);
         Iterable<ServerAddress> members = hashTopologyResp.segments.get(i);
         assertEquals(Math.min(numOwners, ch.getMembers().size()), segment.size());
         int count = 0;
         for (ServerAddress member : members) {
            count++;
            assertTrue(serverAddresses.contains(member));
         }
         // The number of servers could be smaller than the number of CH members (same as the number of actual members)
         assertEquals(Math.min(numOwners, servers.size()), count);
      }
   }

   public static int getServerTopologyId(EmbeddedCacheManager cm, String cacheName) {
      return cm.getCache(cacheName).getAdvancedCache().getRpcManager().getTopologyId();
   }

   public static void killClient(HotRodClient client) {
      try {
         if (client != null) {
            client.stop().await();
         }
      } catch (Throwable t) {
         log.error("Error stopping client", t);
      }
   }

   public static ConfigurationBuilder hotRodCacheConfiguration() {
      return hotRodCacheConfiguration(new ConfigurationBuilder());
   }

   public static ConfigurationBuilder hotRodCacheConfiguration(ConfigurationBuilder builder) {
      return hotRodCacheConfiguration(builder, MediaType.APPLICATION_PROTOSTREAM);
   }

   public static ConfigurationBuilder hotRodCacheConfiguration(ConfigurationBuilder cfg, MediaType types) {
      cfg.encoding().key().mediaType(types.toString());
      cfg.encoding().value().mediaType(types.toString());
      return cfg;
   }

   public static ConfigurationBuilder hotRodCacheConfiguration(MediaType types) {
      return hotRodCacheConfiguration(new ConfigurationBuilder(), types);
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, byte[] key, byte[] expectedValue) {
      return assertHotRodEquals(cm.getCache(), key, expectedValue);
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String cacheName,
                                               byte[] key, byte[] expectedValue) {
      return assertHotRodEquals(cm.getCache(cacheName), key, expectedValue);
   }

   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String key, String expectedValue) {
      return assertHotRodEquals(cm.getCache(), marshall(key), marshall(expectedValue));
   }


   public static CacheEntry assertHotRodEquals(EmbeddedCacheManager cm, String cacheName,
                                               String key, String expectedValue) {
      return assertHotRodEquals(cm.getCache(cacheName), marshall(key), marshall(expectedValue));
   }

   private static CacheEntry assertHotRodEquals(Cache<byte[], byte[]> cache,
                                                byte[] key, byte[] expectedValue) {
      AdvancedCache advancedCache = cache.getAdvancedCache().withStorageMediaType();
      CacheEntry<byte[], byte[]> entry = advancedCache.getCacheEntry(key);
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
         return obj == null ? null : getMarshaller().objectToByteBuffer(obj, 64);
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }


   public static <T> T unmarshall(byte[] key) {
      try {
         return (T) getMarshaller().objectFromByteBuffer(key);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   private static Marshaller getMarshaller() {
      // Must check for GenericJbossMarshaller as infinispan-jboss-marshalling still used by client
      Marshaller marshaller = Util.getJBossMarshaller(HotRodTestingUtil.class.getClassLoader(), null);
      return marshaller != null ? marshaller : new ProtoStreamMarshaller();
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

   static final AtomicInteger uniqueAddr = new AtomicInteger(12411);

   static class UniquePortThreadLocal extends ThreadLocal<Integer> {
      @Override
      protected Integer initialValue() {
         int port = uniqueAddr.getAndAdd(110);
         log.debugf("Server port range for test thread %s is: %d-%d", Thread.currentThread().getId(), port, port + 109);
         return port;
      }
   }
}
