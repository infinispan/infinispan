package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

@CleanupAfterMethod
@Test(testName = "client.hotrod.impl.transport.netty.CrashMidOperationTest", groups = "functional")
public class CrashMidOperationTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      builder.maxRetries(0);
   }

   public void killServerMidOperation() throws Exception {
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      CountDownLatch operationLatch = new CountDownLatch(1);
      AtomicReference<Channel> channelRef = new AtomicReference<>();
      ExecutorService operationsExecutor = Executors.newFixedThreadPool(2);

      NoopRetryingOperation firstOperation = new NoopRetryingOperation(0, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, operationLatch);
      operationsExecutor.submit(() -> channelFactory.fetchChannelAndInvoke(address, firstOperation));

      eventually(() -> channelRef.get() != null);
      Channel channel = channelRef.get();

      HotRodClientTestingUtil.killServers(hotRodServer1);
      eventually(() -> !channel.isActive());

      eventually(firstOperation::isDone);
      Exceptions.expectExecutionException(TransportException.class, firstOperation);

      // Since the first operation failed midway execution, we don't know if the server has failed or only the channel.
      // The second operation will try to connect and fail, and then update the failed list.
      NoopRetryingOperation secondOperation = new NoopRetryingOperation(1, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, operationLatch);
      channelFactory.fetchChannelAndInvoke(address, remoteCache.getName().getBytes(StandardCharsets.UTF_8), secondOperation);
      eventually(secondOperation::isDone);
      try {
         secondOperation.get(10, TimeUnit.SECONDS);
      } catch (Throwable t) {
         assertTrue(t.getCause() instanceof ConnectException);
      }

      // We only release the latch now, but notice that all the other operations were able to finish.
      operationLatch.countDown();

      // The failed list was update, the next operation should succeed.
      NoopRetryingOperation thirdOperation = new NoopRetryingOperation(2, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, operationLatch);
      channelFactory.fetchChannelAndInvoke(address, remoteCache.getName().getBytes(StandardCharsets.UTF_8), thirdOperation);
      eventually(thirdOperation::isDone);
      thirdOperation.get(10, TimeUnit.SECONDS);
      operationsExecutor.shutdown();
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

         try {
            firstOp.await();
            complete(null);
         } catch (InterruptedException e) {
            completeExceptionally(e);
         }
      }

      @Override
      public void writeBytes(Channel channel, ByteBuf buf) {
         if (channelRef.compareAndSet(null, channel)) {
            try {
               firstOp.await();
            } catch (InterruptedException e) {
               completeExceptionally(e);
            }
            assert isDone() : "Should be done";
            return;
         }

         try {
            firstOp.await();
            complete(null);
         } catch (InterruptedException e) {
            completeExceptionally(e);
         }
      }

      @Override
      public String toString() {
         return "id = " + id;
      }
   }
}
