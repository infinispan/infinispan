package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Connections are shared by all caches of a {@code RemoteCacheManager}. When caches run on an asymmetric set of
 * servers, a topology update that drops a server from one cache must not close the shared connection while another
 * cache still uses that server, otherwise healthy connections are repeatedly closed and reopened (ISPN-18549).
 */
@Test(groups = "unit", testName = "client.hotrod.impl.transport.netty.AsymmetricTopologyConnectionSharingTest")
public class AsymmetricTopologyConnectionSharingTest extends AbstractInfinispanTest {
   private static final InetSocketAddress A = InetSocketAddress.createUnresolved("localhost", 11222);
   private static final InetSocketAddress B = InetSocketAddress.createUnresolved("localhost", 11223);
   private static final InetSocketAddress[] SERVERS = {A, B};

   private OperationDispatcher dispatcher() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host(A.getHostString()).port(A.getPort());
      builder.addServer().host(B.getHostString()).port(B.getPort());
      return new OperationDispatcher(builder.build(), mock(ExecutorService.class), new DefaultTimeService(),
            mock(ClientListenerNotifier.class), null);
   }

   private Channel connect(OperationDispatcher dispatcher, SocketAddress address) {
      Channel channel = new EmbeddedChannel();
      ChannelRecord.set(channel, address);
      OperationChannel connection = mock(OperationChannel.class);
      when(connection.getChannel()).thenReturn(channel);
      when(connection.isAcceptingRequests()).thenReturn(true);
      when(connection.pendingOperationFlowable()).thenReturn(Flowable.empty());
      when(connection.close()).thenAnswer(invocation -> {
         channel.close();
         return List.of();
      });
      connections(dispatcher).put(address, connection);
      return channel;
   }

   private ConcurrentMap<SocketAddress, OperationChannel> connections(OperationDispatcher dispatcher) {
      return TestingUtil.extractField(dispatcher.getChannelHandler(), "channels");
   }

   private OperationChannel channel(OperationDispatcher dispatcher, SocketAddress address) {
      return dispatcher.getChannelHandler().getChannelForAddress(address);
   }

   private void dropToServerA(OperationDispatcher dispatcher, String cacheName, int topologyId) {
      dispatcher.addCacheTopologyInfoIfAbsent(cacheName);
      dispatcher.updateTopology(cacheName, mock(HotRodOperation.class), topologyId,
            new InetSocketAddress[]{A}, new SocketAddress[0][], (short) -1);
   }

   /**
    * A topology that drops server B from one cache keeps the shared connection open as long as any other cache
    * still lists B, and closes it only when the last such cache drops it.
    */
   public void testConnectionClosedOnlyByLastCacheUsingIt() {
      OperationDispatcher dispatcher = dispatcher();
      connect(dispatcher, A);
      connect(dispatcher, B);
      OperationChannel channelB = connections(dispatcher).get(B);
      // The default cache and two named caches all start on the initial server list {A, B}.
      dispatcher.addCacheTopologyInfoIfAbsent("cacheA");
      dispatcher.addCacheTopologyInfoIfAbsent("cacheB");

      // cacheB learns it only runs on A. B is still used by the default cache and cacheA, so its connection stays.
      dropToServerA(dispatcher, "cacheB", 1);
      assertThat(channel(dispatcher, B)).as("B still used by other caches").isNotNull();
      verify(channelB, never()).close();

      // cacheA also drops B, but the default cache still lists it.
      dropToServerA(dispatcher, "cacheA", 1);
      assertThat(channel(dispatcher, B)).as("B still used by the default cache").isNotNull();
      verify(channelB, never()).close();

      // The last cache using B drops it, so the shared connection is finally closed.
      dropToServerA(dispatcher, "", 1);
      assertThat(channel(dispatcher, B)).as("no cache uses B anymore").isNull();
      verify(channelB).close();

      // A is used by every cache the whole time and is never closed.
      assertThat(channel(dispatcher, A)).isNotNull();
   }

   /**
    * With a single cache the connection to a dropped server is closed immediately, confirming the reference-count
    * guard does not keep connections open when no other cache uses the server.
    */
   public void testConnectionClosedWhenNoOtherCacheUsesServer() {
      OperationDispatcher dispatcher = dispatcher();
      connect(dispatcher, A);
      connect(dispatcher, B);
      OperationChannel channelB = connections(dispatcher).get(B);

      // Only the default cache exists; dropping B leaves no cache using it.
      dropToServerA(dispatcher, "", 1);
      assertThat(channel(dispatcher, B)).isNull();
      verify(channelB).close();
   }

   /**
    * The primary-listener replacement path ({@code closeExcludedChannels}) must apply the same rule: a server
    * excluded from one cache's topology is not disconnected while another cache still uses it.
    */
   public void testExcludedChannelKeptOpenWhileUsedByAnotherCache() {
      OperationDispatcher dispatcher = dispatcher();
      Channel oldA = connect(dispatcher, A);
      connect(dispatcher, B);
      OperationChannel channelB = connections(dispatcher).get(B);
      dispatcher.addCacheTopologyInfoIfAbsent("cacheX");
      dispatcher.addCacheTopologyInfoIfAbsent("cacheY");

      // Establish a primary-listener sequence for cacheX, then let a response arrive from a replacement connection
      // that excludes B. cacheY still runs on B, so B's connection must stay open.
      dispatcher.updatePrimaryTopology("cacheX", oldA, 100, -1, new BitSet(), 123, SERVERS,
            new SocketAddress[][]{{A}}, (short) 3);
      Channel newA = connect(dispatcher, A);
      dispatcher.updateTopology("cacheX", null, 456, new InetSocketAddress[]{A},
            new SocketAddress[][]{{A}}, (short) 3, newA);

      assertThat(channel(dispatcher, B)).as("B still used by cacheY").isNotNull();
      verify(channelB, never()).close();
   }
}
