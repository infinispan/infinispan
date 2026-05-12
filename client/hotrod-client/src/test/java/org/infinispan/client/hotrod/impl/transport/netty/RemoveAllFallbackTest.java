package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.TestResourceTracker;
import org.infinispan.testing.skip.StringLogAppender;
import org.testng.annotations.Test;

/**
 * Verifies that {@code removeAll} falls back to individual remove operations
 * when the server does not advertise support for the REMOVE_ALL opcode.
 */
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.RemoveAllFallbackTest")
public class RemoveAllFallbackTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private StringLogAppender logAppender;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      clientBuilder.connectionPool().maxActive(1).minIdle(1);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      String testShortName = TestResourceTracker.getCurrentTestShortName();
      logAppender = new StringLogAppender("org.infinispan.HOTROD_ACCESS_LOG",
            Level.TRACE,
            t -> t.getName().startsWith("non-blocking-thread-" + testShortName),
            PatternLayout.newBuilder().setPattern("%X{method}").build());
      logAppender.install();
      super.setup();
   }

   @Override
   protected void teardown() {
      super.teardown();
      logAppender.uninstall();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testRemoveAllFallsBackToIndividualRemoves() {
      RemoteCache<String, String> cache = remoteCacheManager.getCache();

      cache.put("a", "1");
      cache.put("b", "2");
      cache.put("c", "3");
      assertEquals(3, cache.size());

      simulateServerWithoutRemoveAll();

      // removeAll should fall back to individual remove operations
      cache.removeAllAsync(Set.of("a", "b")).toCompletableFuture().join();

      assertThat(cache).doesNotContainKeys("a", "b");
      assertEquals("3", cache.get("c"));

      Map<String, Integer> ops = new HashMap<>();
      for (int i = 0; i < logAppender.size(); i++) {
         ops.compute(logAppender.get(i), (k, v) -> v == null ? 1 : v + 1);
      }
      assertThat(ops.get("REMOVE")).isEqualTo(2);
      assertThat(ops).doesNotContainKey("REMOVE_ALL");
   }

   /**
    * Replaces the OperationChannel's codec with one that has the REMOVE_ALL bit
    * cleared from its supported ops, simulating a connection to an older server.
    */
   private void simulateServerWithoutRemoveAll() {
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotrodServer.getHost(), hotrodServer.getPort());
      OperationDispatcher dispatcher = TestingUtil.extractField(remoteCacheManager, "dispatcher");
      OperationChannel operationChannel = dispatcher.getHandlerForAddress(address);

      BitSet currentOps = TestingUtil.extractField(operationChannel.codec, "supportedOps");
      BitSet reducedOps = (BitSet) currentOps.clone();
      reducedOps.clear(HotRodConstants.REMOVE_ALL_REQUEST);

      Codec oldServerCodec = ProtocolVersion.PROTOCOL_VERSION_41.getCodec(reducedOps);
      CompletableFuture<Void> future = new CompletableFuture<>();
      operationChannel.getChannel().eventLoop().execute(() -> {
         try {
            operationChannel.setCodec(oldServerCodec);
            future.complete(null);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      });
      future.join();

      assertFalse(operationChannel.isOpSupported(HotRodConstants.REMOVE_ALL_REQUEST));
      assertTrue(operationChannel.isOpSupported(HotRodConstants.REMOVE_REQUEST));
   }
}
