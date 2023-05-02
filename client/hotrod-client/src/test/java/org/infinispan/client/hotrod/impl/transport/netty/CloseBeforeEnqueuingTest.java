package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   protected RemoteCacheManager createRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      amendRemoteCacheManagerConfiguration(builder);
      builder
            .forceReturnValues(true)
            .connectionTimeout(5)
            .connectionPool().maxActive(1) // This ensures that only one server is active at a time
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
      ExecutorService operationsExecutor = Executors.newSingleThreadExecutor();

      NoopRetryingOperation firstOperation = new NoopRetryingOperation(0, channelFactory, remoteCacheManager.getConfiguration(),
            channelRef, operationLatch);
      operationsExecutor.submit(() -> channelFactory.fetchChannelAndInvoke(address, firstOperation));

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
      operationsExecutor.submit(() -> channelFactory.fetchChannelAndInvoke(address, secondOperation));
      secondOperation.get(10, TimeUnit.SECONDS);
      operationsExecutor.shutdownNow();
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
      public void writeBytes(Channel channel, ByteBuf buf) {
         throw new UnsupportedOperationException("TODO!");
      }

      @Override
      public String toString() {
         return "id = " + id;
      }
   }

   private static class CustomChannelFactory extends ChannelFactory {

      private final Configuration configuration;
      private Supplier<Boolean> executeInstead;

      public CustomChannelFactory(Configuration cfg) {
         super(new CodecHolder(cfg.version().getCodec()));
         this.configuration = cfg;
         this.executeInstead = null;
      }

      public void setExecuteInstead(Supplier<Boolean> supplier) {
         this.executeInstead = supplier;
      }

      @Override
      protected V2ChannelPool createChannelPool(Bootstrap bootstrap, ChannelInitializer channelInitializer, SocketAddress address) {
         int maxConnections = configuration.connectionPool().maxActive();
         if (maxConnections < 0) {
            maxConnections = Integer.MAX_VALUE;
         }
         return new V2ChannelPool(bootstrap.config().group().next(), address, channelInitializer,
               configuration.connectionPool().exhaustedAction(), this::onConnectionEvent,
               configuration.connectionPool().maxWait(), maxConnections,
               configuration.connectionPool().maxPendingRequests()) {

            @Override
            boolean executeDirectlyIfPossible(ChannelOperation callback) {
               if (executeInstead != null && !executeInstead.get()) {
                  return false;
               }
               return super.executeDirectlyIfPossible(callback);
            }
         };
      }
   }
}
