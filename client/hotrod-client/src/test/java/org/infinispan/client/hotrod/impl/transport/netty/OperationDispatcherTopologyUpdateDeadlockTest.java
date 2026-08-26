package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.newRemoteConfigurationBuilder;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.EventLoopGroup;

/**
 * Reproducer for the self-deadlock on the non-reentrant {@link java.util.concurrent.locks.StampedLock} used by
 * {@link OperationDispatcher}.
 * <p>
 * A topology update takes the write lock in {@link OperationDispatcher#updateTopology} and, while still holding it,
 * creates a channel for a newly added server via {@code updateCacheInfo -> startChannelIfNeeded}. If that connection
 * fails synchronously, the failure handler used to run inline on the same thread and re-acquire the write lock through
 * {@link OperationDispatcher#handleConnectionFailure}. Because the lock is not reentrant, the thread parked forever
 * against itself.
 * <p>
 * The test forces the synchronous connection failure deterministically by shutting down the event loop group, so any
 * new connection attempt fails immediately.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/18003">#18003</a>
 */
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.OperationDispatcherTopologyUpdateDeadlockTest")
public class OperationDispatcherTopologyUpdateDeadlockTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hotrodServer = startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = newRemoteConfigurationBuilder();
      clientBuilder.addServer().host(hotrodServer.getHost()).port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void shutDownHotrod() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testTopologyUpdateAddingUnreachableServerDoesNotDeadlock() throws Exception {
      // Warm up so the default cache has a topology and an established channel.
      remoteCacheManager.getCache().put("key", "value");

      OperationDispatcher dispatcher = TestingUtil.extractField(remoteCacheManager, "dispatcher");

      // Force any new connection attempt to fail synchronously: with the event loop group shut down, the channel can no
      // longer be registered so bootstrap.connect() completes exceptionally right away, exercising the immediate-error
      // path in OperationChannel.attemptConnect().
      EventLoopGroup eventLoopGroup = dispatcher.getChannelHandler().getEventLoopGroup();
      eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
      assertThat(eventLoopGroup.awaitTermination(30, TimeUnit.SECONDS))
            .as("event loop group should terminate")
            .isTrue();

      // Build a topology update that keeps the current servers and adds a new one. The added server triggers
      // startChannelIfNeeded while updateTopology holds the write lock.
      List<InetSocketAddress> newServers = new ArrayList<>(dispatcher.getServers(HotRodConstants.DEFAULT_CACHE_NAME));
      newServers.add(InetSocketAddress.createUnresolved("127.0.0.1", hotrodServer.getPort() + 1));
      InetSocketAddress[] addresses = newServers.toArray(new InetSocketAddress[0]);
      int newTopologyId = dispatcher.getTopologyId(HotRodConstants.DEFAULT_CACHE_NAME) + 1;

      // Run the topology update on a separate thread; if the code self-deadlocks it will never return and this thread
      // stays alive. hashFunctionVersion < 0 keeps the update on the simpler withNewServers path.
      Future<Void> future = fork(() ->
            dispatcher.updateTopology(HotRodConstants.DEFAULT_CACHE_NAME, null, newTopologyId,
                  addresses, new SocketAddress[0][], (short) -1));
      future.get(10, TimeUnit.SECONDS);
   }
}
