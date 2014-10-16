package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.AssertJUnit.assertNull;

/**
 * State transfer and replication queue test verifying that the interaction between them two works in fine.
 * <p/>
 * In particular, this test focuses on checking that ordering is maintained when multiple operations are executed
 * on the same key in a asynchronous environment with async marshalling turned off.
 *
 * @author Galder Zamarre�o
 * @author Dan Berindei
 * @since 4.1
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "statetransfer.StateTransferReplicationQueueTest")
public class StateTransferReplicationQueueTest extends MultipleCacheManagersTest {

   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;

   private final String TX_CACHE = "txCache";
   private final String NONTX_CACHE = "nontxCache";

   protected void createCacheManagers() throws Throwable {
      // The cache managers are created in the test methods
   }

   private Configuration buildConfiguration(boolean tx) {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, tx);
      cb.clustering()
            .async().useReplQueue(true)
            .replQueueInterval(100)
            .replQueueMaxElements(3);
      // These are the default:
      // .asyncMarshalling(false)
      // .stateTransfer().fetchInMemoryState(true)
      // .locking().useLockStriping(false);
      return cb.build();
   }

   public void testStateTransferWithNodeRestartedAndBusyTx(Method m) throws Exception {
      log.info(m.getName() + " start");
      doWritingCacheTest(TX_CACHE, true);
      log.info(m.getName() + "end");
   }

   public void testStateTransferWithNodeRestartedAndBusyImplicitTx(Method m) throws Exception {
      log.info(m.getName() + " start");
      doWritingCacheTest(TX_CACHE, false);
      log.info(m.getName() + "end");
   }

   public void testStateTransferWithNodeRestartedAndBusyNonTx(Method m) throws Exception {
      log.info(m.getName() + " start");
      doWritingCacheTest(NONTX_CACHE, false);
      log.info(m.getName() + "end");
   }

   private void doWritingCacheTest(String cacheName, boolean tx) throws InterruptedException {
      // Start the first cache
      final EmbeddedCacheManager manager1 = createCacheManager();
      Cache<Object, Object> cache1 = manager1.getCache(cacheName);

      TestingUtil.replaceComponent(manager1, ClusterTopologyManager.class, new DelayingClusterTopologyManager(manager1), true);

      // Start the second cache
      EmbeddedCacheManager manager2 = createCacheManager();
      manager2.getCache(cacheName);

      writeInitialData(cache1);


      WritingThread writerThread = new WritingThread(cache1, tx);
      writerThread.start();

      manager2.stop();

      // Pause for view to update
      TestingUtil.blockUntilViewsReceived(60000, false, cache1);
      TestingUtil.waitForRehashToComplete(cache1);

      EmbeddedCacheManager manager3 = createCacheManager();
      Cache<Object, Object> cache3 = manager3.getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache3);
      TestingUtil.waitForRehashToComplete(cache1, cache3);

      writerThread.stopThread();
      writerThread.join(60000);

      verifyInitialData(cache3);

      int count = writerThread.result();

      // Wait for the replication queue to be emptied
      final ReplicationQueue replQueue1 = cache1.getAdvancedCache().getComponentRegistry().getComponent(ReplicationQueue.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return replQueue1.getElementsCount() == 0;
         }
      });

      // Wait a little longer, even the replication queue sends the commands asynchronously
      Thread.sleep(1000);

      for (int c = 0; c < count; c++) {
         Object o = cache3.get("test" + c);
         // Nothing should be left after a put/remove on a key
         assertNull(o);
      }
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(TX_CACHE, buildConfiguration(true));
      cm.defineConfiguration(NONTX_CACHE, buildConfiguration(true));
      return cm;
   }

   protected void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
   }

   protected void verifyInitialData(Cache<Object, Object> c) {
      assert JOE.equals(c.get(A_B_NAME)) : "Incorrect value for key " + A_B_NAME;
      assert TWENTY.equals(c.get(A_B_AGE)) : "Incorrect value for key " + A_B_AGE;
      assert BOB.equals(c.get(A_C_NAME)) : "Incorrect value for key " + A_C_NAME;
      assert FORTY.equals(c.get(A_C_AGE)) : "Incorrect value for key " + A_C_AGE;
   }

   private static class WritingThread extends Thread {
      private final Cache<Object, Object> cache;
      private final boolean tx;
      private volatile boolean stop;
      private volatile int result;
      private TransactionManager tm;

      WritingThread(Cache<Object, Object> cache, boolean tx) {
         super("WriterThread");
         this.cache = cache;
         this.tx = tx;
         if (tx) tm = TestingUtil.getTransactionManager(cache);
         setDaemon(true);
      }

      public int result() {
         return result;
      }

      public void run() {
         int c = 0;
         while (!stop) {
            try {
               if (tx) tm.begin();
               cache.put("test" + c, new PojoValue(c));
               cache.remove("test" + c);
               if (tx) tm.commit();
               c++;
               if (c % 1000 == 0) TestingUtil.sleepThread(1); // Slow it down a bit
            } catch (Exception e) {
               stopThread();
            }
         }
         result = c;
      }

      public void stopThread() {
         stop = true;
      }
   }

   public static class PojoValue implements Externalizable {
      static AtomicBoolean holdUp = new AtomicBoolean();
      Log log = LogFactory.getLog(PojoValue.class);
      volatile int value;

      public PojoValue() {
      }

      public PojoValue(int value) {
         this.value = value;
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {
         String threadName = Thread.currentThread().getName();
         if (!holdUp.get()) {
            log.debug("In streaming...");
            holdUp.compareAndSet(false, true);
            log.debug("Holding up...");
            TestingUtil.sleepThread(1000); // Sleep for 2 seconds to hold up state transfer
         }

         out.writeInt(value);
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         value = in.readInt();
      }

      @Override
      public int hashCode() {
         return value + 31;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         PojoValue pojo = (PojoValue) o;
         if (value != pojo.value) return false;
         return true;
      }
   }


   private class DelayingClusterTopologyManager implements ClusterTopologyManager {
      private final EmbeddedCacheManager manager1;
      private ClusterTopologyManager instance;

      public DelayingClusterTopologyManager(EmbeddedCacheManager manager1) {
         this.manager1 = manager1;
         instance = TestingUtil.extractGlobalComponent(manager1, ClusterTopologyManager.class);
      }

      @Override
      public org.infinispan.topology.CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception {
         CacheStatusResponse result = instance.handleJoin(cacheName, joiner, joinInfo, viewId);

         // Allow the joiner to receive some commands before the initial cache topology
         log.tracef("Delaying join response");
         Thread.sleep(500);
         return result;
      }

      @Override
      public void handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
         instance.handleLeave(cacheName, leaver, viewId);
      }

      @Override
      public void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
         instance.handleRebalanceCompleted(cacheName, node, topologyId, throwable, viewId);
      }

      @Override
      public void handleClusterView(boolean isMerge, int viewId) {
         instance.handleClusterView(isMerge, viewId);
      }

      @Override
      public void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
         // Allow the joiner to receive some commands between the initial cache topology and the rebalance start
         log.tracef("Delaying rebalance");
         try {
            Thread.sleep(500);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }

         instance.broadcastRebalanceStart(cacheName, cacheTopology, totalOrder, distributed);
      }

      @Override
      public void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, boolean totalOrder, boolean distributed) {
         instance.broadcastTopologyUpdate(cacheName, cacheTopology, availabilityMode, totalOrder, distributed);
      }

      @Override
      public void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
         instance.broadcastStableTopologyUpdate(cacheName, cacheTopology, totalOrder, distributed);
      }

      @Override
      public boolean isRebalancingEnabled() {
         return instance.isRebalancingEnabled();
      }

      @Override
      public void setRebalancingEnabled(boolean enabled) {
         instance.setRebalancingEnabled(enabled);
      }

      @Override
      public void forceRebalance(String cacheName) {
         instance.forceRebalance(cacheName);
      }

      @Override
      public void forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode) {
         instance.forceAvailabilityMode(cacheName, availabilityMode);
      }
   }
}
