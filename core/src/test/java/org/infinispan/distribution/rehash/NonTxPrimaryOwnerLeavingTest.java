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
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
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

      // Block remote put commands invoked from cache0
      ControlledRpcManager crm = new ControlledRpcManager(cache0.getRpcManager());
      cache0.getComponentRegistry().registerComponent(crm, RpcManager.class);
      cache0.getComponentRegistry().rewire();
      crm.blockBefore(operation.getCommandClass());

      // Try to put a key/value from cache0 with cache1 the primary owner
      final MagicKey key = new MagicKey(cache1);
      Future<Object> future = fork(() -> operation.perform(cache0, key));

      // After the put command was sent, kill cache1
      crm.waitForCommandToBlock();
      TopologyUpdateListener listener = new TopologyUpdateListener();
      cache0.addListener(listener);

      cache1.stop();

      if (!blockTopologyOnOriginator) {
         listener.preLatch.open();
         assertFalse(listener.broken);
         if (!listener.postLatch.await(10, TimeUnit.SECONDS)) {
            throw new TimeoutException();
         }
      }

      // Now that cache1 is stopped, unblock the put command
      crm.stopBlocking();

      if (blockTopologyOnOriginator) {
         // The write can't finish until the originator has the new topology
         // So we have to unblock it eventually, but only after NonTxDistributionInterceptor finishes the RPC
         TestingUtil.sleepThread(500);
         listener.preLatch.open();
         assertFalse(listener.broken);
      }

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertNull(result);
      log.tracef("Write operation is done");

      // Check the value on the remaining node
      assertEquals(operation.getValue(), cache0.get(key));
      assertEquals(operation.getValue(), cache2.get(key));
   }

   @Listener
   public static class TopologyUpdateListener {
      final ReclosableLatch preLatch = new ReclosableLatch();
      final ReclosableLatch postLatch = new ReclosableLatch();
      volatile boolean broken = false;

      @TopologyChanged
      public void onTopologyChange(TopologyChangedEvent e) throws InterruptedException {
         if (e.isPre()) {
            broken = !preLatch.await(10, TimeUnit.SECONDS);
         } else {
            postLatch.open();
         }
      }
   }
}
