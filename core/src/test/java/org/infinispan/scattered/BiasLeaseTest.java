package org.infinispan.scattered;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.RenewBiasCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.CountingRpcManager;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scattered.BiasLeaseTest")
public class BiasLeaseTest extends MultipleCacheManagersTest {
   private static final long BIAS_LIFESPAN = ClusteringConfiguration.BIAS_LIFESPAN.getDefaultValue();
   private ControlledRpcManager rpcManager0;
   private CountingRpcManager rpcManager1;
   private ControlledTimeService timeService = new ControlledTimeService();
   private RenewWaitingInvocationHandler handler0;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      builder.clustering().biasAcquisition(BiasAcquisition.ON_WRITE);
      // Scan biases frequently
      builder.expiration().wakeUpInterval(100);
      createCluster(builder, 3);

      IntStream.of(0, 1, 2).mapToObj(this::cache).forEach(
            c -> TestingUtil.replaceComponent(c, TimeService.class, timeService, true));
      rpcManager0 = ControlledRpcManager.replaceRpcManager(cache(0));
      rpcManager1 = CountingRpcManager.replaceRpcManager(cache(1));
      TestingUtil.wrapInboundInvocationHandler(cache(0), handler -> handler0 = new RenewWaitingInvocationHandler(handler));
   }

   @AfterMethod
   public void cleanup() {
      rpcManager0.excludeCommands(ClearCommand.class);
      IntStream.of(0, 1, 2).mapToObj(this::cache).forEach(Cache::clear);
      rpcManager0.excludeCommands();
   }

   public void testBiasTimesOut() throws Exception {
      rpcManager0.excludeCommands(PrimaryAckCommand.class, ExceptionAckCommand.class);
      MagicKey key = new MagicKey(cache(0));
      Future<Object> putFuture = fork(() -> cache(1).put(key, "v0"));
      putFuture.get(10, TimeUnit.SECONDS);

      assertTrue(biasManager(1).hasLocalBias(key));

      timeService.advance(BIAS_LIFESPAN + 1);
      rpcManager0.expectCommand(RevokeBiasCommand.class).send().receiveAll();

      // The local bias is invalidated synchronously with the sync command, but it may take few moments
      // to remove the remote bias.
      assertFalse(biasManager(1).hasLocalBias(key));
      eventuallyEquals(null, () -> biasManager(0).getRemoteBias(key));
   }

   public void testBiasLeaseRenewed() throws Exception {
      MagicKey key = new MagicKey(cache(0));
      Future<Object> putFuture = fork(() -> cache(1).put(key, "v0"));
      rpcManager0.expectCommand(PrimaryAckCommand.class).sendWithoutResponses();
      putFuture.get(10, TimeUnit.SECONDS);

      assertEquals(Collections.singletonList(address(1)), biasManager(0).getRemoteBias(key));
      assertTrue(biasManager(1).hasLocalBias(key));

      for (int i = 0; i < 3; ++i) {
         CountDownLatch latch = new CountDownLatch(1);
         handler0.latch = latch;
         timeService.advance(BIAS_LIFESPAN - 1);

         rpcManager1.resetStats();
         assertEquals("v0", cache(1).get(key));
         assertEquals(0, rpcManager1.clusterGet);
         assertTrue(latch.await(30, TimeUnit.SECONDS));

         assertEquals(Collections.singletonList(address(1)), biasManager(0).getRemoteBias(key));
      }
   }

   protected BiasManager biasManager(int index) {
      return cache(index).getAdvancedCache().getComponentRegistry().getComponent(BiasManager.class);
   }

   private class RenewWaitingInvocationHandler implements PerCacheInboundInvocationHandler {
      private final PerCacheInboundInvocationHandler delegate;
      private volatile CountDownLatch latch;

      private RenewWaitingInvocationHandler(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         CountDownLatch latch = this.latch;
         if (command instanceof RenewBiasCommand && latch != null) {
            delegate.handle(command, response -> {
               reply.reply(response);
               this.latch = null;
               latch.countDown();
            }, order);
         } else {
            delegate.handle(command, reply, order);
         }
      }
   }
}
