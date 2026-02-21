package org.infinispan.statetransfer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

/**
 * tests scenario for ISPN-2574
 *
 * - create nodes A, B - start node C - starts state transfer from B to C
 * - abruptly kill B before it is able to send StateResponse to C
 * - C resends the request to A
 * - finally cluster A, C is formed where all entries are properly backed up on both nodes
 *
 * @author Michal Linhard
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferRestartTest")
@CleanupAfterMethod
public class StateTransferRestartTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder cfgBuilder;

   @Override
   protected void createCacheManagers() throws Throwable {
      cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfgBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cfgBuilder.clustering().hash().numOwners(2);
      cfgBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      cfgBuilder.clustering().stateTransfer().timeout(20000);
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   public void testStateTransferRestart() {
      final int numKeys = 100;

      addClusterEnabledCacheManager(cfgBuilder, new TransportFlags().withFD(true));
      addClusterEnabledCacheManager(cfgBuilder, new TransportFlags().withFD(true));
      log.info("waiting for cluster { c0, c1 }");
      waitForClusterToForm();

      log.info("putting in data");
      final Cache<Object, Object> c0 = cache(0);
      final Cache<Object, Object> c1 = cache(1);
      for (int k = 0; k < numKeys; k++) {
         c0.put(k, k);
      }
      TestingUtil.waitForNoRebalance(c0, c1);

      assertEquals(numKeys, c0.entrySet().size());
      assertEquals(numKeys, c1.entrySet().size());

      PerCacheInboundInvocationHandler spyHandler0 = Mocks.replaceComponentWithSpy(c1, PerCacheInboundInvocationHandler.class);
      doAnswer(invocation -> {
         log.info("KILLING the c1 cache");
         try {
            DISCARD d3 = TestingUtil.getDiscardForCache(c1.getCacheManager());
            d3.discardAll(true);
            TestingUtil.killCacheManagers(manager(c1));
         } catch (Exception e) {
            log.info("there was some exception while killing cache");
         }
         return null;
      })
      // Only on first call - when c2 joins do we kill the cache
      .doCallRealMethod().when(spyHandler0).handle(any(InitialPublisherCommand.class), any(Reply.class), any(DeliverOrder.class));

      log.info("adding cache c2");
      addClusterEnabledCacheManager(cfgBuilder, new TransportFlags().withFD(true));

      log.info("get c2");
      final Cache<Object, Object> c2 = cache(2);

      log.info("waiting for cluster { c0, c2 }");
      TestingUtil.blockUntilViewsChanged(10000, 2, c0, c2);

      log.infof("c0 entrySet size before : %d", c0.entrySet().size());
      log.infof("c2 entrySet size before : %d", c2.entrySet().size());

      eventuallyEquals(numKeys, () -> c0.entrySet().size());
      eventuallyEquals(numKeys, () -> c2.entrySet().size());

      log.info("Ending the test");
   }

   private RequestRepository spyRequestRepository(EmbeddedCacheManager cm) {
      Transport transport = TestingUtil.extractGlobalComponent(cm, Transport.class);
      return Mocks.replaceFieldWithSpy(transport, "requests");
   }
}
