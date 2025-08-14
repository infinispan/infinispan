package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(testName = "client.hotrod.impl.transport.netty.ChannelCloseAndInactiveTest", groups = "functional")
public class ChannelCloseAndInactiveTest extends AbstractRetryTest {

   {
      nbrOfServers = 1;
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      System.setProperty("io.netty.eventLoopThreads", "4");
   }

   @Override
   protected boolean cleanupAfterTest() {
      System.clearProperty("io.netty.eventLoopThreads");
      return super.cleanupAfterTest();
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      builder.maxRetries(1);
      // We manually stall operations, so let's define something that won't timeout.
      builder.socketTimeout(60_000);
      builder.connectionPool().maxActive(2);
   }

   public void testKillAndInactiveDifferentChannelsConcurrently() throws Exception {
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      CountDownLatch operationLatch = new CountDownLatch(1);

      // We proceed to create two different channels.
      AtomicReference<Channel> firstChannelRef = new AtomicReference<>();
      AtomicReference<Channel> secondChannelRef = new AtomicReference<>();

      CrashMidOperationTest.NoopRetryingOperation firstOperation = new CrashMidOperationTest.NoopRetryingOperation(0, channelFactory, remoteCacheManager.getConfiguration(),
            firstChannelRef, operationLatch);
      fork(() -> channelFactory.fetchChannelAndInvoke(address, firstOperation));

      // Wait to acquire the first channel.
      eventually(() -> firstChannelRef.get() != null);
      Channel firstChannel = firstChannelRef.get();

      // Eventually the Noop operation is registered.
      HeaderDecoder firstDecoder = ((HeaderDecoder) firstChannel.pipeline().get(HeaderDecoder.NAME));
      eventually(() -> firstDecoder.registeredOperations() == 1);

      // The first channel does not return to the pool. We submit the second operation to create a new channel.
      CrashMidOperationTest.NoopRetryingOperation secondOperation = new CrashMidOperationTest.NoopRetryingOperation(1, channelFactory, remoteCacheManager.getConfiguration(),
            secondChannelRef, operationLatch);
      fork(() -> channelFactory.fetchChannelAndInvoke(address, secondOperation));

      // Wait to acquire the second channel.
      eventually(() -> secondChannelRef.get() != null);
      Channel secondChannel = secondChannelRef.get();

      // The first decoder still has the operation registered.
      assertThat(firstDecoder.registeredOperations()).isOne();

      // With some operations queued on the first channel, we release the second to the pool.
      // We are going to spy it here, so we don't invoke close right away, we want it to return as active first.
      Channel spyChannel = spy(secondChannel);
      CountDownLatch closeSecondLatch = new CountDownLatch(1);
      doAnswer(ivk -> {
         assertThat(closeSecondLatch.await(10, TimeUnit.SECONDS)).isTrue();
         return ivk.callRealMethod();
      }).when(spyChannel).close();

      // Now some checks on the pool. There is no idle channel, as both are working on the operations.
      ChannelPool pool = ChannelRecord.of(firstChannel).channelPool();
      assertThat(pool.getIdle()).isZero();
      assertThat(pool.getConnected()).isEqualTo(2);

      // After returning the channel to the pool, we have an idle channel to be utilized.
      ChannelRecord.of(secondChannel).release(spyChannel);
      assertThat(pool.getIdle()).isOne();

      // This will cause the HeaderDecoder to be removed.
      // This would also invoke close, but since we're spying, we can delay that a little.
      assertThat(secondOperation.isDone()).isFalse();

      Future<Void> exceptionCaught = fork(() -> {
         secondOperation.exceptionCaught(secondChannel, new TransportException("oops", address));
      });

      // Now that the second channel is on the pool, we close the first channel.
      // This will cause the enqueued operations to retry.
      firstChannel.close().awaitUninterruptibly();
      assertThat(firstChannel.isActive()).isFalse();

      // Now allow the channel to close, so we don't leak anything.
      closeSecondLatch.countDown();

      // After closing the FIRST channel, BOTH operation will complete successfully.
      eventually(() -> pool.getConnected() > 0);
      eventually(exceptionCaught::isDone);
      exceptionCaught.get(10, TimeUnit.SECONDS);

      eventually(firstOperation::isDone);
      eventually(secondOperation::isDone);

      // Release all operations!
      operationLatch.countDown();

      secondOperation.get(10, TimeUnit.SECONDS);
      firstOperation.get(10, TimeUnit.SECONDS);
   }
}
