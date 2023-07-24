package org.infinispan.client.hotrod.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.impl.protocol.Codec25;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@TestForIssue(jiraKey = "ISPN-14846")
@CleanupAfterMethod
@Test(groups = "functional", testName = "client.hotrod.retry.ClientListenerFailoverBusyTest")
public class ClientListenerFailoverBusyTest extends AbstractRetryTest {

   private static final int MAX_PENDING_REQUESTS = 10;

   {
      nbrOfServers = 1;
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected RemoteCacheManager createClient() {
      RemoteCacheManager rcm = super.createClient();
      rcm.getChannelFactory().setNegotiatedCodec(new Codec25());
      return rcm;
   }

   @Override
   protected void amendRemoteCacheManagerConfiguration(org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder) {
      builder.version(ProtocolVersion.PROTOCOL_VERSION_25)
            // We need maxActive=2 because protocol version is 2.5.
            // The listener never releases its channel to the pool.
            .connectionPool().maxActive(2)

            // Necessary so the listener reuses the channel.
            .maxPendingRequests(MAX_PENDING_REQUESTS + 1);
   }

   public void testWithASingleOperation() throws Exception {
      testListenerWithSlowServer(1);
   }

   public void testWithMultipleOperations() throws Exception {
      testListenerWithSlowServer(MAX_PENDING_REQUESTS);
   }

   private void testListenerWithSlowServer(int numberOfOperations) throws Exception {
      AdvancedCache<?, ?> cache = cacheToHit(1);
      InetSocketAddress address = InetSocketAddress.createUnresolved(hotRodServer1.getHost(), hotRodServer1.getPort());

      // We acquire the channel. This will be the same channel for the listener.
      Channel channel = channelFactory.fetchChannelAndInvoke(address, new NoopChannelOperation()).get(10, TimeUnit.SECONDS);

      // Release channel back to the pool so listener can use it.
      ChannelRecord.of(channel).release(channel);

      Listener listener = new Listener();
      remoteCache.addClientListener(listener);

      ExecutorService executor = Executors.newScheduledThreadPool(numberOfOperations);
      CyclicBarrier barrier = new CyclicBarrier(numberOfOperations + 1);
      DelayedInterceptor interceptor = new DelayedInterceptor(barrier, executor);

      TestingUtil.extractInterceptorChain(cache)
                  .addInterceptor(interceptor, 1);

      // We issue all of these operations which do not complete until the latch is released.
      AggregateCompletionStage<?> operations = CompletionStages.aggregateCompletionStage();
      for (int i = 0; i < numberOfOperations; i++) {
         operations.dependsOn(remoteCache.putAsync(1, "v" + i));
      }

      int eventsBeforeFailover = listener.getReceived();
      assertThat(eventsBeforeFailover).isZero();
      assertThat(listener.didFailover()).isFalse();

      // Now close the listener channel, so it failover to the channel with put operations.
      channel.close().awaitUninterruptibly();
      eventually(() -> channelFactory.getNumActive() == 1);
      eventually(listener::didFailover);

      try {
         // Await all the operations reach the interceptor.
         barrier.await(10, TimeUnit.SECONDS);
         operations.freeze().toCompletableFuture().get(10, TimeUnit.SECONDS);

         // If numberOfOperations == 1 we only create the entry, we do not generate events from updates.
         if (numberOfOperations > 1)
            eventually(() -> String.format("Never got more events: %d of %d", eventsBeforeFailover, listener.getReceived()),
                  () -> listener.getReceived() > eventsBeforeFailover);
      } finally {
         TestingUtil.extractInterceptorChain(cache).removeInterceptor(DelayedInterceptor.class);
         executor.shutdown();
      }
   }


   @ClientListener
   private static class Listener {

      private final AtomicInteger count = new AtomicInteger(0);
      private final AtomicBoolean failover = new AtomicBoolean(false);

      @ClientCacheEntryModified
      public void handleModifiedEvent(ClientCacheEntryModifiedEvent<?> ignore) {
         count.incrementAndGet();
      }

      @ClientCacheFailover
      public void handleFailoverEvent(ClientCacheFailoverEvent ignore) {
         failover.set(true);
      }

      int getReceived() {
         return count.intValue();
      }

      boolean didFailover() {
         return failover.get();
      }
   }

   public static class DelayedInterceptor extends DDAsyncInterceptor {
      private final CyclicBarrier barrier;
      private final ExecutorService executor;

      public DelayedInterceptor(CyclicBarrier barrier, ExecutorService executor) {
         this.barrier = barrier;
         this.executor = executor;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         CompletableFuture<Object> cf = new CompletableFuture<>();
         executor.submit(() -> {
            try {
               barrier.await();
               cf.complete(super.visitPutKeyValueCommand(ctx, command));
            } catch (Throwable e) {
               cf.completeExceptionally(e);
            }
         });
         return asyncValue(cf);
      }
   }
}
