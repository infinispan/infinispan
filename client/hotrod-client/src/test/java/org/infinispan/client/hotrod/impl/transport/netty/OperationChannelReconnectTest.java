package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.newRemoteConfigurationBuilder;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;

@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.OperationChannelReconnectTest")
public class OperationChannelReconnectTest extends SingleCacheManagerTest {

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

   public void testStaleDrainRunsAgainstChannelInstalledByReconnect() throws Exception {
      // Warm up so channel A is connected on its own event loop.
      remoteCacheManager.getCache().put("key", "value");

      InetSocketAddress address =
            InetSocketAddress.createUnresolved(hotrodServer.getHost(), hotrodServer.getPort());
      OperationDispatcher dispatcher = TestingUtil.extractField(remoteCacheManager, "dispatcher");
      OperationChannel operationChannel = dispatcher.getHandlerForAddress(address);

      Channel channelA = operationChannel.getChannel();
      assertThat(channelA).isNotNull();
      EventLoop loopA = channelA.eventLoop();

      CheckPoint checkPoint = new CheckPoint();

      // Replace only the SEND_OPERATIONS runnable with something that delays the call.
      // The real runnable is kept and invoked later; the wrapper defers the *first* drain (the one channel A scheduled) until we release the checkpoint.
      Runnable realSendOperations = TestingUtil.extractField(operationChannel, "SEND_OPERATIONS");
      AtomicBoolean firstDrain = new AtomicBoolean(true);
      AtomicReference<CompletableFuture<Void>> staleOutcome = new AtomicReference<>();
      Runnable deferringSendOperations = () -> {
         if (firstDrain.getAndSet(false)) {
            // Wait for the event to simulate the task was scheduled but delayed.
            // It was scheduled in A's event loop, so we dispatch it in that same event loop.
            staleOutcome.set(checkPoint.future0("release_stale", 1).thenRunAsync(realSendOperations, loopA));
            checkPoint.trigger("stale_deferred");
         } else {
            realSendOperations.run();
         }
      };
      TestingUtil.replaceField(deferringSendOperations, "SEND_OPERATIONS", operationChannel, OperationChannel.class);

      // (1) Channel A schedules a drain on its own loop, exactly as sendOperation() does. The wrapper
      //     defers it and returns, leaving loop A free.
      HotRodOperation<?> mockHrOp = mock(HotRodOperation.class);
      operationChannel.sendOperation(mockHrOp);
      checkPoint.awaitStrict("stale_deferred", 10, TimeUnit.SECONDS);

      // (2) Reconnect on loop A (its own loop, as a dying channel would).
      // Simulates a failure happening concurrently.
      // This method would run from A's event loop.
      loopA.submit(() -> {
         Iterable<HotRodOperation<?>> ops = operationChannel.reconnect(new TransportException("channel A died", address));
         assertThat(ops).hasSize(1);
      }).get(10, TimeUnit.SECONDS);

      // Eventually the new channel is created.
      eventually(() -> {
         Channel current = operationChannel.getChannel();
         return current != null && current != channelA && operationChannel.isAcceptingRequests();
      });
      Channel channelB = operationChannel.getChannel();
      assertThat(channelB.eventLoop())
            .as("reconnect must install channel B on a different loop, else there is no cross-thread drain")
            .isNotSameAs(loopA);

      // (3) Release the deferred drain:
      // the sendOperations that was scheduled in A's event loop is running now in a thread which is not the current channel's event loop.
      // This allows for multiple threads to consume the MPSC queue and cause undefined behaviour.
      // The method should not perform any operation and return without.
      HotRodOperation<?> otherHrOp = mock(HotRodOperation.class);
      // Include a task in the queue directly so we can assert it is not consumed.
      operationChannel.pendingChannelOperations().offer(otherHrOp);
      checkPoint.trigger("release_stale");
      staleOutcome.get().get(10, TimeUnit.SECONDS);

      // Operation was not consumed by the consumer running in A's event loop.
      assertThat(operationChannel.pendingChannelOperations())
            .hasSize(1)
            .contains(otherHrOp);
   }
}
