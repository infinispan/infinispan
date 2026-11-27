package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

import io.netty.channel.EventLoopGroup;

/**
 * Verifies that {@code removeAll} targeting a server whose channel is still connecting waits for the connection,
 * instead of reading the channel's codec before it is set.
 * <p>
 * A server added to the topology gets a new {@link OperationChannel}, which has no codec until the connection
 * completes and the protocol is negotiated. The client runs on a single event loop, which the test blocks, so the new
 * channel stays in that state while {@code removeAll} is invoked.
 * </p>
 */
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.RemoveAllConnectingServerTest")
public class RemoveAllConnectingServerTest extends MultiHotRodServersTest {

   private static final String CACHE_NAME = HotRodConstants.DEFAULT_CACHE_NAME;
   private static final long TIMEOUT_SECONDS = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServersWithoutClients(2, hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC)));
   }

   public void testRemoveAllWaitsForConnectingServer() throws Exception {
      RemoteCacheManager client = createSingleEventLoopClient();
      OperationDispatcher dispatcher = TestingUtil.extractField(client, "dispatcher");
      // A custom transport factory owns its event loop group, so the client doesn't shut it down
      EventLoopGroup eventLoopGroup = dispatcher.getChannelHandler().getEventLoopGroup();
      CheckPoint checkPoint = new CheckPoint();
      try {
         RemoteCache<String, String> cache = client.getCache();
         Set<String> keys = putKeys(cache);
         InetSocketAddress first = address(dispatcher, server(0));
         InetSocketAddress second = address(dispatcher, server(1));
         eventually(() -> isAcceptingRequests(dispatcher, first) && isAcceptingRequests(dispatcher, second));

         blockEventLoop(eventLoopGroup, checkPoint);
         try {
            int numSegments = dispatcher.getCacheInfo(CACHE_NAME).getNumSegments();
            rejoinWhileConnecting(dispatcher, first, second, numSegments);

            CompletableFuture<Void> removeAll = cache.removeAllAsync(keys);
            checkPoint.triggerForever("event_loop_released");
            removeAll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
         } finally {
            checkPoint.triggerForever("event_loop_released");
         }

         assertRemoved(cache, keys);
      } finally {
         killRemoteCacheManager(client);
         eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
   }

   public void testRemoveAllRetriesWhenConnectingServerLeaves() throws Exception {
      RemoteCacheManager client = createSingleEventLoopClient();
      OperationDispatcher dispatcher = TestingUtil.extractField(client, "dispatcher");
      // A custom transport factory owns its event loop group, so the client doesn't shut it down
      EventLoopGroup eventLoopGroup = dispatcher.getChannelHandler().getEventLoopGroup();
      CheckPoint checkPoint = new CheckPoint();
      try {
         RemoteCache<String, String> cache = client.getCache();
         Set<String> keys = putKeys(cache);
         InetSocketAddress first = address(dispatcher, server(0));
         InetSocketAddress second = address(dispatcher, server(1));
         eventually(() -> isAcceptingRequests(dispatcher, first) && isAcceptingRequests(dispatcher, second));

         blockEventLoop(eventLoopGroup, checkPoint);
         try {
            int numSegments = dispatcher.getCacheInfo(CACHE_NAME).getNumSegments();
            rejoinWhileConnecting(dispatcher, first, second, numSegments);

            CompletableFuture<Void> removeAll = cache.removeAllAsync(keys);
            assertThat(removeAll)
                  .as("removeAll while the owner's channel is connecting")
                  .isNotDone();

            // The server leaves before its channel connects. Closing the channel ends the wait without a codec
            injectTopology(dispatcher, new InetSocketAddress[]{first}, first, numSegments);

            checkPoint.triggerForever("event_loop_released");
            removeAll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
         } finally {
            checkPoint.triggerForever("event_loop_released");
         }

         assertRemoved(cache, keys);
      } finally {
         killRemoteCacheManager(client);
         eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
   }

   /**
    * Creates a client that knows both servers and runs every channel on a single event loop thread.
    */
   private RemoteCacheManager createSingleEventLoopClient() {
      ConfigurationBuilder builder = createHotRodClientConfigurationBuilder(server(0));
      builder.addServer().host(server(1).getHost()).port(server(1).getPort());
      builder.transportFactory(new SingleEventLoopTransportFactory());
      return new RemoteCacheManager(builder.build());
   }

   private static Set<String> putKeys(RemoteCache<String, String> cache) {
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 5; i++) {
         String key = "k" + i;
         keys.add(key);
         cache.put(key, "v" + i);
      }
      return keys;
   }

   private static void assertRemoved(RemoteCache<String, String> cache, Set<String> keys) {
      for (String key : keys) {
         assertThat(cache.get(key))
               .as("value of %s after removeAll", key)
               .isNull();
      }
   }

   /**
    * Removes {@code second} from the topology and adds it back as the owner of every segment. Must be invoked with
    * the event loop blocked, so the new channel for {@code second} stays connecting.
    */
   private static void rejoinWhileConnecting(OperationDispatcher dispatcher, InetSocketAddress first,
                                             InetSocketAddress second, int numSegments) {
      // The server leaves the topology: its channel is removed
      injectTopology(dispatcher, new InetSocketAddress[]{first}, first, numSegments);
      assertThat(dispatcher.getHandlerForAddress(second))
            .as("channel of the server removed from the topology")
            .isNull();

      // The server joins again, owning every segment. The new channel can't connect while the loop is blocked
      injectTopology(dispatcher, new InetSocketAddress[]{first, second}, second, numSegments);
      OperationChannel connecting = dispatcher.getHandlerForAddress(second);
      assertThat(connecting)
            .as("channel of the server added to the topology")
            .isNotNull();
      assertThat(connecting.isAcceptingRequests())
            .as("new channel accepting requests while the event loop is blocked")
            .isFalse();
   }

   /**
    * Returns the address the client uses for the given server, as listed in the cache topology.
    */
   private static InetSocketAddress address(OperationDispatcher dispatcher, HotRodServer server) {
      InetSocketAddress expected = InetSocketAddress.createUnresolved(server.getHost(), server.getPort());
      List<InetSocketAddress> servers = dispatcher.getServers(CACHE_NAME);
      assertThat(servers)
            .as("servers in the client topology")
            .contains(expected);
      return servers.get(servers.indexOf(expected));
   }

   private static boolean isAcceptingRequests(OperationDispatcher dispatcher, InetSocketAddress address) {
      OperationChannel channel = dispatcher.getHandlerForAddress(address);
      return channel != null && channel.isAcceptingRequests();
   }

   /**
    * Installs a topology with the given servers, where {@code owner} is the only owner of every segment.
    */
   private static void injectTopology(OperationDispatcher dispatcher, InetSocketAddress[] servers,
                                      InetSocketAddress owner, int numSegments) {
      SocketAddress[][] segmentOwners = new SocketAddress[numSegments][];
      Arrays.fill(segmentOwners, new SocketAddress[]{owner});
      int topologyId = dispatcher.getTopologyId(CACHE_NAME) + 1;
      dispatcher.updateTopology(CACHE_NAME, null, topologyId, servers, segmentOwners, (short) 3);
   }

   /**
    * Occupies the client's only event loop thread until {@code event_loop_released} is triggered.
    */
   private static void blockEventLoop(EventLoopGroup eventLoopGroup, CheckPoint checkPoint) throws Exception {
      eventLoopGroup.next().execute(() -> {
         checkPoint.trigger("event_loop_blocked");
         try {
            checkPoint.awaitStrict("event_loop_released", TIMEOUT_SECONDS, TimeUnit.SECONDS);
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      });
      checkPoint.awaitStrict("event_loop_blocked", TIMEOUT_SECONDS, TimeUnit.SECONDS);
   }

   /**
    * Runs every client channel on a single event loop thread, so blocking that thread holds all connection attempts.
    */
   private static final class SingleEventLoopTransportFactory extends DefaultTransportFactory {
      @Override
      public EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
         return super.createEventLoopGroup(1, executorService);
      }
   }
}
