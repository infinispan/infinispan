package org.infinispan.client.hotrod.impl.transport.netty;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.metrics.RemoteCacheManagerMetricsRegistry;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

@CleanupAfterMethod
@Test(testName = "client.hotrod.impl.transport.netty.CloseBeforeEnqueuingTest", groups = "functional")
public class CloseBeforeEnqueuingTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      amendRemoteCacheManagerConfiguration(builder);
      builder
            .forceReturnValues(true)
            .connectionTimeout(5)
            .connectionPool().maxActive(1)
            .addServer().host("127.0.0.1").port(port);
      Configuration configuration = builder.build();
      RemoteCacheManager remoteCacheManager = new InternalRemoteCacheManager(configuration, new CustomChannelFactory(configuration));
      remoteCacheManager.start();
      return remoteCacheManager;
   }

   public void testClosingAndEnqueueing() throws Exception {
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      CountDownLatch operationLatch = new CountDownLatch(1);
      AtomicReference<Channel> channelRef = new AtomicReference<>();

      NoopRetryingOperation firstOperation = new NoopRetryingOperation(0, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, operationLatch);
      fork(() -> channelFactory.fetchChannelAndInvoke(address, firstOperation));

      eventually(() -> channelRef.get() != null);
      Channel channel = channelRef.get();

      assertTrue(channelFactory instanceof CustomChannelFactory);

      AtomicBoolean closedServer = new AtomicBoolean(false);
      ((CustomChannelFactory) channelFactory).setExecuteInstead(() -> {
         HotRodClientTestingUtil.killServers(hotRodServer1);
         eventually(() -> !channel.isActive());
         eventually(() -> channelFactory.getNumActive(address) == 0);
         return !closedServer.compareAndSet(false, true);
      });

      operationLatch.countDown();
      NoopRetryingOperation secondOperation = new NoopRetryingOperation(0, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, null);
      fork(() -> channelFactory.fetchChannelAndInvoke(address, secondOperation));
      secondOperation.get(10, TimeUnit.SECONDS);
   }

   public void testEnqueueAndReleasing() throws Exception {
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      CompletableFuture<Channel> firstOp = new CompletableFuture<>();

      // We issue an operation to acquire the first channel.
      fork(() -> channelFactory.fetchChannelAndInvoke(address, new AcquireChannelOperation(firstOp)));

      // Wait for it to complete.
      Channel firstChannel = firstOp.get(10, TimeUnit.SECONDS);

      // We have a maximum limit of two channels.
      // The next operation is enqueued.
      CheckPoint checkPoint = new CheckPoint();
      ControlledChannelOperation operation = new ControlledChannelOperation(channelFactory, remoteCacheManager.getConfiguration(), checkPoint);

      // We make sure that:
      // 1. The first check does not find an available channel;
      // 2. The operation is enqueued to execute later;
      // 3. We close one of the acquired channels, which picks the operation from the queue;
      // 4. Release the second channel to the pool.
      // The operation should execute only once.
      AtomicBoolean onlyOnce = new AtomicBoolean(true);
      ((CustomChannelFactory) channelFactory).setExecuteInstead(() -> {
         if (!onlyOnce.get()) {
            // The operation is now enqueued, we release the channel to the pool.
            fork(() -> ChannelRecord.of(firstChannel).release(firstChannel));

            try {
               checkPoint.trigger("before_execute_operation");
               checkPoint.awaitStrict("invoke_execute_operation", 10, TimeUnit.SECONDS);
            } catch (Exception ignore) { }
            return true;
         }

         return !onlyOnce.getAndSet(false);
      });

      // With everything in place, issue the request.
      Future<ControlledChannelOperation> invoking = fork(() -> channelFactory.fetchChannelAndInvoke(address, operation));

      // The operation was enqueued, we release a channel to the pool.
      // The channel we released acquire the operation from the queue and is trying to execute it.
      checkPoint.awaitStrict(ControlledChannelOperation.BEFORE_SCHEDULE_READ + 0, 10, TimeUnit.SECONDS);

      // It is verifying for the second time *after* the operation was enqueued.
      // We block at this point until we release the channel again.
      checkPoint.awaitStrict("before_execute_operation", 10, TimeUnit.SECONDS);

      // Allow it to proceed and schedule a read.
      // Eventually it completes, and automatically returns to the pool.
      checkPoint.trigger(ControlledChannelOperation.PROCEED_SCHEDULE_READ + 0);
      eventually(operation::isDone);

      // The operation executed only once.
      operation.assertThatExecutedOnlyOnce();

      // Now we allow the operation in the pool to continue.
      // The operation should not execute again.
      checkPoint.trigger("invoke_execute_operation");

      // And the operation is scheduled only once.
      operation.assertThatExecutedOnlyOnce();

      // Wait for the submit command to finish executing.
      // After it finishes, the operation should already be finished and only once.
      eventually(invoking::isDone);
      assertThat(operation.isDone()).isTrue();
      operation.assertThatExecutedOnlyOnce();

      // We assert the channel we released is still in the pool.
      assertThat(channelFactory.getNumIdle(address)).isOne();
   }

   private static class NoopRetryingOperation extends RetryOnFailureOperation<Void> {
      private final AtomicReference<Channel> channelRef;
      private final CountDownLatch firstOp;
      private final int id;

      protected NoopRetryingOperation(int nbr, ChannelFactory channelFactory, Configuration cfg,
                                      AtomicReference<Channel> channelRef, CountDownLatch firstOp) {
         super((short) 0, (short) 0, null, channelFactory, null,
               new AtomicReference<>(new ClientTopology(-1, cfg.clientIntelligence())), 0, cfg,
               DataFormat.builder().build(), null);
         this.channelRef = channelRef;
         this.firstOp = firstOp;
         this.id = nbr;
      }

      @Override
      public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
         complete(null);
      }

      @Override
      protected void executeOperation(Channel channel) {
         if (channelRef.compareAndSet(null, channel)) {
            try {
               scheduleRead(channel);
               firstOp.await();
            } catch (InterruptedException e) {
               completeExceptionally(e);
            }
            assert isDone() : "Should be done";
            return;
         }

         complete(null);
      }

      @Override
      public String toString() {
         return "id = " + id;
      }
   }

   private static class ControlledChannelOperation extends RetryOnFailureOperation<Void> {
      private static final String BEFORE_SCHEDULE_READ = "before-schedule-read-";
      private static final String PROCEED_SCHEDULE_READ = "proceed-schedule-read-";
      private final AtomicInteger counter = new AtomicInteger(0);

      private final CheckPoint checkPoint;

      protected ControlledChannelOperation(ChannelFactory channelFactory, Configuration cfg, CheckPoint checkPoint) {
         super((short) 0, (short) 0, null, channelFactory, null,
               new AtomicReference<>(new ClientTopology(-1, cfg.clientIntelligence())), 0, cfg,
               DataFormat.builder().build(), null);
         this.checkPoint = checkPoint;
      }

      @Override
      public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
         complete(null);
      }

      @Override
      protected void executeOperation(Channel channel) {
         int execution = counter.getAndIncrement();
         checkPoint.trigger(BEFORE_SCHEDULE_READ + execution);

         try {
            checkPoint.awaitStrict(PROCEED_SCHEDULE_READ + execution, 10, TimeUnit.SECONDS);
            scheduleRead(channel);
         } catch (Exception e) {
            completeExceptionally(e);
         }


         complete(null);
      }

      public void assertThatExecutedOnlyOnce() {
         assertThat(counter.get()).withFailMessage("Operation executed more than once!")
               .isOne();
      }
   }

   private static class CustomChannelFactory extends ChannelFactory {

      private final Configuration configuration;
      private Supplier<Boolean> executeInstead;

      public CustomChannelFactory(Configuration cfg) {
         super(cfg, new CodecHolder(cfg.version().getCodec()));
         this.configuration = cfg;
         this.executeInstead = null;
      }

      public void setExecuteInstead(Supplier<Boolean> supplier) {
         this.executeInstead = supplier;
      }

      @Override
      protected ChannelPool createChannelPool(Bootstrap bootstrap, ChannelInitializer channelInitializer, SocketAddress address) {
         int maxConnections = configuration.connectionPool().maxActive();
         if (maxConnections < 0) {
            maxConnections = Integer.MAX_VALUE;
         }
         return new ChannelPool(bootstrap.config().group().next(), address, channelInitializer,
               configuration.connectionPool().exhaustedAction(), this::onConnectionEvent,
               configuration.connectionPool().maxWait(), maxConnections,
               configuration.connectionPool().maxPendingRequests(),
               RemoteCacheManagerMetricsRegistry.DISABLED) {

            @Override
            boolean executeDirectlyIfPossible(ChannelOperation callback, boolean checkCallback) {
               if (executeInstead != null && !executeInstead.get()) {
                  return false;
               }
               return super.executeDirectlyIfPossible(callback, checkCallback);
            }
         };
      }
   }
}
