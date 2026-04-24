package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.Op;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;

/**
 * Tests that {@link ConnectionMetadata#protocolVersion()} is correctly updated
 * on every request, not just on PING.
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConnectionMetadataTest")
public class HotRodConnectionMetadataTest extends HotRodSingleNodeTest {

   public void testProtocolVersionSetOnPing() {
      // The default client (v2.1) sent a PING on startup
      ConnectionMetadata metadata = getConnectionMetadata(client());
      assertEquals("HOTROD/2.1", metadata.protocolVersion());
   }

   public void testProtocolVersionSetOnNonPingRequest(Method m) {
      // Create a client at version 3.0 without sending a startup PING.
      // Before the fix, ConnectionMetadata.protocolVersion was only set during PING processing,
      // so a client that never sends PING would have null protocolVersion.
      try (HotRodClient client30 = new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName,
            HotRodVersion.HOTROD_30.getVersion(), false)) {
         // Send a PUT at version 3.0 — this should set the metadata
         assertStatus(client30.put(k(m), 0, 0, v(m)), Success);

         ConnectionMetadata metadata = getConnectionMetadata(client30);
         assertEquals("HOTROD/3.0", metadata.protocolVersion());
      }
   }

   public void testProtocolVersionReflectsCorrectVersion() {
      // Create a v3.1 client without PING, send a GET, verify the version
      try (HotRodClient client31 = new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName,
            HotRodVersion.HOTROD_31.getVersion(), false)) {
         client31.get("nonexistent");

         ConnectionMetadata metadata = getConnectionMetadata(client31);
         assertEquals("HOTROD/3.1", metadata.protocolVersion());
      }
   }

   public void testProtocolVersionUpdatedOnVersionChange(Method m) {
      // Use a v2.1 client so the Encoder doesn't write media type bytes (added in v2.8).
      // This lets us safely send Ops at different versions < 2.8 on the same connection,
      // simulating what happens during protocol auto-negotiation.
      try (HotRodClient client = new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName,
            HotRodVersion.HOTROD_21.getVersion(), false)) {
         // Send a GET at version 2.1
         Op getV21 = new Op(0xA0, HotRodVersion.HOTROD_21.getVersion(), HotRodConstants.GET_REQUEST,
               cacheName, k(m), 0, 0, null, 0, 0, (byte) 1, 0);
         client.execute(getV21);

         ConnectionMetadata metadata = getConnectionMetadata(client);
         assertEquals("HOTROD/2.1", metadata.protocolVersion());

         // Send a GET at version 2.5 on the SAME connection
         Op getV25 = new Op(0xA0, HotRodVersion.HOTROD_25.getVersion(), HotRodConstants.GET_REQUEST,
               cacheName, k(m), 0, 0, null, 0, 0, (byte) 1, 0);
         client.execute(getV25);

         // Metadata should update to reflect the new version
         assertEquals("HOTROD/2.5", metadata.protocolVersion());
      }
   }

   private ConnectionMetadata getConnectionMetadata(HotRodClient client) {
      ChannelGroup channels = hotRodServer.getTransport().getAcceptedChannels();
      InetSocketAddress clientLocal = (InetSocketAddress) client.getChannel().localAddress();
      for (Channel ch : channels) {
         InetSocketAddress serverRemote = (InetSocketAddress) ch.remoteAddress();
         if (serverRemote != null && serverRemote.getPort() == clientLocal.getPort()) {
            return ConnectionMetadata.getInstance(ch);
         }
      }
      throw new AssertionError("Could not find server-side channel for client connection");
   }
}
