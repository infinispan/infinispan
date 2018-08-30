package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

/**
 * Tests scenario for ISPN-7127
 *
 * - create nodes A, B - start node C - starts state transfer from B to C
 * - abruptly kill B before it is able to reply to the StateRequestCommand from C
 * - C resends the request to A
 * - finally cluster A, C is formed where all entries are properly backed up on both nodes
 *
 * @author Michal Linhard
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferRestart2Test")
@CleanupAfterMethod
public class StateTransferRestart2Test extends MultipleCacheManagersTest {

   private ConfigurationBuilder cfgBuilder;

   @Override
   protected void createCacheManagers() throws Throwable {
      cfgBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfgBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cfgBuilder.clustering().hash().numOwners(2);
      cfgBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      cfgBuilder.clustering().stateTransfer().timeout(20000);

      GlobalConfigurationBuilder gcb0 = new GlobalConfigurationBuilder().clusteredDefault();
      addClusterEnabledCacheManager(gcb0, cfgBuilder, new TransportFlags().withFD(true));
      GlobalConfigurationBuilder gcb1 = new GlobalConfigurationBuilder().clusteredDefault();
      addClusterEnabledCacheManager(gcb1, cfgBuilder, new TransportFlags().withFD(true));
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   public void testStateTransferRestart() throws Throwable {
      final int numKeys = 100;

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

      DISCARD d1 = TestingUtil.getDiscardForCache(c1.getCacheManager());
      GlobalConfigurationBuilder gcb2 = new GlobalConfigurationBuilder();
      gcb2.transport().transport(new JGroupsTransport() {
         @Override
         public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                                     ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                     long timeout, TimeUnit unit) {
            if (command instanceof StateRequestCommand &&
                  ((StateRequestCommand) command).getType() == StateRequestCommand.Type.START_STATE_TRANSFER &&
                  target.equals(address(1))) {
               d1.setDiscardAll(true);

               fork((Callable<Void>) () -> {
                  log.info("KILLING the c1 cache");
                  TestingUtil.killCacheManagers(manager(c1));
                  return null;
               });
            }
            return super.invokeCommand(target, command, collector, deliverOrder, timeout, unit);
         }
      });

      log.info("adding cache c2");
      addClusterEnabledCacheManager(gcb2, cfgBuilder, new TransportFlags().withFD(true));
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
}
