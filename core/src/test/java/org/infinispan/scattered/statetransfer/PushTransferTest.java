package org.infinispan.scattered.statetransfer;

import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DelegatingStateTransferLock;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.AbstractControlledRpcManager;
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
   };

   public void testNodeJoin() throws Exception {
      List<MagicKey> keys = init();
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(defaultConfig, TRANSPORT_FLAGS);
      LocalTopologyManager ltm4 = TestingUtil.extractGlobalComponent(cm4, LocalTopologyManager.class);
      int currentTopologyId = TestingUtil.extractComponent(c1, StateTransferManager.class).getCacheTopology().getTopologyId();

      BlockingLocalTopologyManager bltm = new BlockingLocalTopologyManager(ltm4);
      bltm.startBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
      bltm.startBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
      TestingUtil.replaceComponent(cm4, LocalTopologyManager.class, bltm, true);

      Stream.of(c1, c2, c3).forEach(c -> {
         RpcManager rpcManager = TestingUtil.extractComponent(c, RpcManager.class);
         TestingUtil.replaceComponent(c, RpcManager.class, new AbstractControlledRpcManager(rpcManager) {
            @Override
            protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap, Object argument) {
               if (command instanceof StateResponseCommand && responseMap.keySet().contains(cm4.getAddress())) {
                  // We don't have to wait for all push transfers to finish, one is enough to lose something
                  boolean pushTransfer;
                  try {
                     Field pushTransferField = StateResponseCommand.class.getDeclaredField("pushTransfer");
                     pushTransferField.setAccessible(true);
                     pushTransfer = pushTransferField.getBoolean(command);
                  } catch (Exception e) {
                     throw new IllegalStateException();
                  }
                  if (pushTransfer) {
                     log.info("Push transfer response received, unblocking");
                     bltm.stopBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
                     bltm.stopBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
                  }
               }
               return responseMap;
            }
         }, true);
      });

      TestingUtil.addCacheStartingHook(cm4, (name, cr) -> {
         StateTransferLock stateTransferLock = cr.getComponent(StateTransferLock.class);
         cr.registerComponent(new DelegatingStateTransferLock(stateTransferLock) {
            @Override
            public boolean transactionDataReceived(int expectedTopologyId) {
               if (expectedTopologyId == currentTopologyId  + 1) {
                  // Let the topology update be processed - otherwise we would be blocked forever
                  bltm.stopBlocking(BlockingLocalTopologyManager.LatchType.CONSISTENT_HASH_UPDATE);
                  bltm.stopBlocking(BlockingLocalTopologyManager.LatchType.REBALANCE);
               }
               return super.transactionDataReceived(expectedTopologyId);
            }
         }, StateTransferLock.class);
         cr.rewire();
      });

      Cache c4 = cm4.getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4);

      for (MagicKey key : keys) {
         int copies = Stream.of(c1, c2, c3, c4).mapToInt(c -> c.getAdvancedCache().getDataContainer().containsKey(key) ? 1 : 0).sum();
         assertEquals("Key " + key + " has incorrect number of copies", 2, copies);
      }
   }

}
