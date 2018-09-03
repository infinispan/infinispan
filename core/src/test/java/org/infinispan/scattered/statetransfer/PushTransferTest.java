package org.infinispan.scattered.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.testng.annotations.Test;

/**
 * Checks that push-transfer works correctly, the StateResponseCommand is delayed until the topology
 * which the command belongs to is installed.
 */
@Test(groups = "functional", testName = "scattered.statetransfer.PushTransferTest")
public class PushTransferTest extends AbstractStateTransferTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new PushTransferTest().biasAcquisition(BiasAcquisition.NEVER),
            new PushTransferTest().biasAcquisition(BiasAcquisition.ON_WRITE)
      };
   }

   public void testNodeJoin() throws Exception {
      List<MagicKey> keys = init();
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(defaultConfig, TRANSPORT_FLAGS);
      int startTopologyId = c1.getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();

      BlockingLocalTopologyManager bltm = BlockingLocalTopologyManager.replaceTopologyManager(cm4, CACHE_NAME);

      CountDownLatch statePushedLatch = new CountDownLatch(1);
      CountDownLatch stateAppliedLatch = new CountDownLatch(1);
      TestingUtil.addCacheStartingHook(cm4, (name, cr) -> {
         PerCacheInboundInvocationHandler originalHandler = cr.getComponent(PerCacheInboundInvocationHandler.class);
         BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
         bcr.replaceComponent(PerCacheInboundInvocationHandler.class.getName(), (PerCacheInboundInvocationHandler) (command, reply, order) -> {
            // StateResponseCommand is topology-aware, so handle() just queues it on the remote executor
            if (command instanceof StateResponseCommand) {
               log.tracef("State received on %s", cm4.getAddress());
               statePushedLatch.countDown();
            }
            originalHandler.handle(command, response -> {
               log.tracef("State applied on %s", cm4.getAddress());
               stateAppliedLatch.countDown();
               reply.reply(response);
            }, order);
         }, false);
         cr.rewire();
         cr.cacheComponents();
      });

      Future<Cache> c4Future = fork(() -> cm4.getCache(CACHE_NAME));

      // Any StateResponseCommand should be delayed until node 4 has the TRANSITORY topology
      assertTrue(statePushedLatch.await(10, TimeUnit.SECONDS));
      assertFalse(stateAppliedLatch.await(100, TimeUnit.MILLISECONDS));

      // Finish the rebalance, unblocking the StateResponseCommand(s)
      bltm.confirmTopologyUpdate(CacheTopology.Phase.TRANSITORY);
      assertEquals(0, stateAppliedLatch.getCount());
      bltm.confirmTopologyUpdate(CacheTopology.Phase.NO_REBALANCE);

      Cache c4 = c4Future.get(30, TimeUnit.SECONDS);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4);

      for (MagicKey key : keys) {
         int copies = Stream.of(c1, c2, c3, c4).mapToInt(c -> c.getAdvancedCache().getDataContainer().containsKey(key) ? 1 : 0).sum();
         assertEquals("Key " + key + " has incorrect number of copies", 2, copies);
      }
   }

}
