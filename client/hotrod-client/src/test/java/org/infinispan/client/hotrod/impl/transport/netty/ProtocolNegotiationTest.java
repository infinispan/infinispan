package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.Codec41;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

/**
 * Verifies that protocol version negotiation during the PING handshake correctly
 * updates the HeaderDecoder codec to the negotiated version.
 */
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.ProtocolNegotiationTest")
public class ProtocolNegotiationTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

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

   @AfterClass
   public void shutDownHotrod() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testSetCodecUpdatesHeaderDecoder() {
      // Ensure the connection is established
      RemoteCache<String, String> cache = remoteCacheManager.getCache();
      cache.put("key", "value");

      InetSocketAddress address = InetSocketAddress.createUnresolved(hotrodServer.getHost(), hotrodServer.getPort());
      OperationDispatcher dispatcher = TestingUtil.extractField(remoteCacheManager, "dispatcher");
      OperationChannel operationChannel = dispatcher.getHandlerForAddress(address);
      Channel channel = operationChannel.getChannel();

      HeaderDecoder headerDecoder = channel.pipeline().get(HeaderDecoder.class);
      assertNotNull(headerDecoder);

      // Downgrade the HeaderDecoder codec to the safe handshake version (3.1),
      // simulating the state after a handshake that used the safe codec
      Codec safeCodec = ProtocolVersion.SAFE_HANDSHAKE_PROTOCOL_VERSION.getCodec();
      TestingUtil.replaceField(safeCodec, "codec", headerDecoder, HeaderDecoder.class);

      // Call setCodec with the negotiated codec (4.1) from within the event loop,
      // as setCodec asserts it runs in the event loop
      Codec negotiatedCodec = ProtocolVersion.PROTOCOL_VERSION_41.getCodec();
      CompletableFuture<Void> future = new CompletableFuture<>();
      channel.eventLoop().execute(() -> {
         try {
            headerDecoder.setCodec(negotiatedCodec);
            future.complete(null);
         } catch (Throwable t) {
            future.completeExceptionally(t);
         }
      });
      future.join();

      // Verify the HeaderDecoder codec was updated to Codec41
      Object codec = TestingUtil.extractField(headerDecoder, "codec");
      assertTrue("Expected Codec41 after setCodec but got " + codec.getClass().getSimpleName(),
            codec instanceof Codec41);
   }
}
