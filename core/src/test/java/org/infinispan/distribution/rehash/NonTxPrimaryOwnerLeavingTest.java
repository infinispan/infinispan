package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when the primary owner of a key leaves during a put operation.
 * See https://issues.jboss.org/browse/ISPN-3366
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPrimaryOwnerLeavingTest")
@CleanupAfterMethod
public class NonTxPrimaryOwnerLeavingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   @Test(groups = "unstable")
   public void testPrimaryOwnerLeavingDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE, false);
   }

   public void testPrimaryOwnerLeavingDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT, false);
   }

   public void testPrimaryOwnerLeaveDuringPutAll() throws Exception {
      doTest(TestWriteOperation.PUT_MAP_CREATE, false);
   }

   public void testPrimaryOwnerLeaveDuringPutAll2() throws Exception {
      doTest(TestWriteOperation.PUT_MAP_CREATE, true);
   }

   private void doTest(TestWriteOperation operation, boolean blockTopologyOnOriginator) throws Exception {
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      AdvancedCache<Object, Object> cache1 = advancedCache(1);
      AdvancedCache<Object, Object> cache2 = advancedCache(2);

      TopologyUpdateListener listener0 = new TopologyUpdateListener();
      cache0.addListener(listener0);
      TopologyUpdateListener listener2 = new TopologyUpdateListener();
      cache2.addListener(listener2);

      // Block remote put commands invoked from cache0
      ControlledRpcManager crm = ControlledRpcManager.replaceRpcManager(cache0);
      crm.excludeCommands(StateRequestCommand.class, StateResponseCommand.class);

      // Try to put a key/value from cache0 with cache1 the primary owner
      final MagicKey key = new MagicKey(cache1);
      Future<Object> future = fork(() -> operation.perform(cache0, key));

      // After the write command was sent, kill cache1
      ControlledRpcManager.BlockedRequest blockedWrite = crm.expectCommand(operation.getCommandClass());

      cache1.stop();

      if (!blockTopologyOnOriginator) {
         listener0.unblockOnce();
         listener0.waitForTopologyToFinish();
      }

      // Now that cache1 is stopped, unblock the write command and wait for the responses
      blockedWrite.send().expectResponse(address(1), CacheNotFoundResponse.INSTANCE).receive();

      if (blockTopologyOnOriginator) {
         // The retry should be blocked on the originator until we unblock the topology update
         crm.expectNoCommand(100, TimeUnit.MILLISECONDS);
         listener0.unblockOnce();
         listener0.waitForTopologyToFinish();
      }

      // Install the new topology without cache1 on cache2 as well
      listener2.unblockOnce();
      listener2.waitForTopologyToFinish();

      // Retry the write command with a single owner (rebalance topology is blocked).
      if (!cache0.getDistributionManager().getCacheTopology().getDistribution(key).isPrimary()) {
         crm.expectCommand(operation.getCommandClass()).send().receiveAll();
      }

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertNull(result);
      log.tracef("Write operation is done");

      cache0.removeListener(listener0);
      cache2.removeListener(listener2);
      listener0.unblockOnce();
      listener0.unblockOnce();
      crm.stopBlocking();

      // Check the value on the remaining node
      assertEquals(operation.getValue(), cache0.get(key));
      assertEquals(operation.getValue(), cache2.get(key));
   }

   @Listener
   public class TopologyUpdateListener {
      private final ReclosableLatch preLatch = new ReclosableLatch();
      private final ReclosableLatch postLatch = new ReclosableLatch();
      private volatile boolean broken = false;

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent e) throws InterruptedException {
         if (e.isPre()) {
            log.tracef("Blocking topology %d", e.getNewTopologyId());
            broken = !preLatch.await(10, TimeUnit.SECONDS);
            preLatch.close();
         } else {
            log.tracef("Signalling topology %d finished installing", e.getNewTopologyId());
            postLatch.open();
         }
      }

      void unblockOnce() {
         preLatch.open();
         assertFalse(broken);
      }

      private void waitForTopologyToFinish() throws InterruptedException {
         if (!postLatch.await(10, TimeUnit.SECONDS)) {
            throw new TimeoutException();
         }
         postLatch.close();
      }
   }
}
