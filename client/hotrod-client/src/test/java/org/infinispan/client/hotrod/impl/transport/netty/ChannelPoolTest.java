package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.retry.AbstractRetryTest;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

@CleanupAfterMethod
@Test(groups = "functional", testName = "client.hotrod.impl.transport.netty.ChannelPoolTest")
public class ChannelPoolTest extends AbstractRetryTest {

   private int retries = 0;

   public ChannelPoolTest() {}

   public ChannelPoolTest(int nbrOfServers) {
      this.nbrOfServers = nbrOfServers;
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      builder.maxRetries(retries);
   }

   public void testClosingSockAndKillingServerFinishesOperations() throws Exception {
      doTest(true);
   }

   public void testClosingSockAndKeepingServerFinishesOperations() throws Exception {
      doTest(false);
   }

   private void doTest(boolean killServer) throws Exception {
      ChannelFactory channelFactory = remoteCacheManager.getChannelFactory();
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());
      AggregateCompletionStage<Void> pendingOperations = CompletionStages.aggregateCompletionStage();
      AtomicReference<Channel> channelRef = new AtomicReference<>();

      CountDownLatch firstOp = new CountDownLatch(1);
      ExecutorService operationsExecutor = Executors.newFixedThreadPool(2); // The fist operation blocks.

      for (int i = 0; i < 10; i++) {
         NoopRetryingOperation op = new NoopRetryingOperation(i, channelFactory, remoteCacheManager.getConfiguration(), channelRef, firstOp);
         operationsExecutor.submit(() -> channelFactory.fetchChannelAndInvoke(address, op));
         pendingOperations.dependsOn(op);
      }

      eventually(() -> channelRef.get() != null);
      Channel channel = channelRef.get();

      if (killServer) HotRodClientTestingUtil.killServers(hotRodServer1);
      channel.close().awaitUninterruptibly();

      // The first one completes successfully on server1.
      firstOp.countDown();

      if (nbrOfServers == 1 && killServer) {
         assertConnectException(pendingOperations);
         operationsExecutor.shutdown();
         return;
      }

      if (retries == 0 && killServer) {
         assertConnectException(pendingOperations);
         operationsExecutor.shutdown();
         return;
      }

      pendingOperations.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);
      operationsExecutor.shutdown();
   }

   private void assertConnectException(AggregateCompletionStage<Void> ops) {
      try {
         ops.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
         Throwable cause = e.getCause();
         if (cause instanceof ConnectException) {
            return;
         }
         throw new AssertionError("Expected ConnectException, but got " + cause, cause);
      }
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
               firstOp.await();
               complete(null);
            } catch (InterruptedException e) {
               completeExceptionally(e);
            }
         } else {
            complete(null);
         }
      }

      @Override
      public void writeBytes(Channel channel, ByteBuf buf) {
         throw new UnsupportedOperationException("Should not ever be invoked!");
      }

      @Override
      public String toString() {
         return "id = " + id;
      }
   }

   private ChannelPoolTest withRetries(int retries) {
      this.retries = retries;
      return this;
   }

   @Override
   protected String parameters() {
      return "[retries=" + retries + ", nbrServers=" + nbrOfServers + "]";
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ChannelPoolTest().withRetries(0),
            new ChannelPoolTest(1).withRetries(0),
            new ChannelPoolTest().withRetries(10),
            new ChannelPoolTest(1).withRetries(10),
      };
   }
}
