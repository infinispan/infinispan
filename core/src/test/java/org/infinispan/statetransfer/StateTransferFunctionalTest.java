package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@Test(groups = "functional", testName = "statetransfer.StateTransferFunctionalTest")
public class StateTransferFunctionalTest extends MultipleCacheManagersTest {

   public static final String A_B_NAME = "a_b_name";
   public static final String A_C_NAME = "a_c_name";
   public static final String A_D_NAME = "a_d_age";
   public static final String A_B_AGE = "a_b_age";
   public static final String A_C_AGE = "a_c_age";
   public static final String A_D_AGE = "a_d_age";
   public static final String JOE = "JOE";
   public static final String BOB = "BOB";
   public static final String JANE = "JANE";
   public static final Integer TWENTY = 20;
   public static final Integer FORTY = 40;

   protected ConfigurationBuilder configurationBuilder;
   protected final String cacheName;

   private volatile int testCount = 0;

   private static final Log log = LogFactory.getLog(StateTransferFunctionalTest.class);

   public StateTransferFunctionalTest() {
      this("nbst");
   }

   public StateTransferFunctionalTest(String testCacheName) {
      cacheName = testCacheName;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .useSynchronization(false)
            .recovery().disable();
      configurationBuilder.clustering().sync().replTimeout(30000);
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      configurationBuilder.locking().useLockStriping(false); // reduces the odd chance of a key collision and deadlock
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(configurationBuilder, new TransportFlags().withMerge(true));
      return cm;
   }

   public static class DelayTransfer implements Serializable {

      private static final long serialVersionUID = 6361429803359702822L;
      
      private volatile boolean doDelay = false;

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
         in.defaultReadObject();
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();

         if (doDelay) {
            try {
               // Delay state transfer
               Thread.sleep(2000);
            }
            catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }

      public void enableDelay() {
         doDelay = true;
      }
   }

   private static class WritingTask implements Callable<Integer> {
      private final Cache<Object, Object> cache;
      private final boolean tx;
      private volatile boolean stop;
      private TransactionManager tm;

      WritingTask(Cache<Object, Object> cache, boolean tx) {
         this.cache = cache;
         this.tx = tx;
         if (tx) tm = TestingUtil.getTransactionManager(cache);
      }

      @Override
      public Integer call() throws Exception {
         int c = 0;
         while (!stop) {
            boolean success = false;
            try {
               if (tx)
                  tm.begin();
               cache.put("test" + c, c);
               if (tx)
                  tm.commit();
               success = true;
               c++;

               // Without this, the writing thread would occupy 1 core completely before the 2nd node joins.
               Thread.sleep(1);
            } catch (Exception e) {
               log.errorf(e, "Error writing key test%s", c);
               stop();
            } finally {
               if (tx && !success) {
                  try {
                     tm.rollback();
                  } catch (SystemException e) {
                     log.error(e);
                  }
               }
            }
         }
         return c;
      }

      public void stop() {
         stop = true;
      }
   }

   public void testInitialStateTransfer(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      EmbeddedCacheManager cm1 = createCacheManager();
      cache1 = cm1.getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);
      node.verifyStateTransfer(new CacheVerifier(cache2));

      logTestEnd(m);
   }

   public void testInitialStateTransferCacheNotPresent(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      EmbeddedCacheManager cacheManager1 = createCacheManager();
      cache1 = cacheManager1.getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node = new JoiningNode(createCacheManager());
      cache2 = node.getCache(cacheName);
      node.waitForJoin(60000, cache1, cache2);
      node.verifyStateTransfer(new CacheVerifier(cache2));

      cacheManager1.defineConfiguration("otherCache", configurationBuilder.build());
      cacheManager1.getCache("otherCache");
      logTestEnd(m);
   }

   public void testConcurrentStateTransfer(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2, cache3, cache4;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);

      cache1.put("delay", new DelayTransfer());

      node2.waitForJoin(60000, cache1, cache2);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      final JoiningNode node3 = new JoiningNode(createCacheManager());
      final JoiningNode node4 = new JoiningNode(createCacheManager());

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            node3.getCache(cacheName);
         }
      });
      t1.setName("CacheStarter-Cache3");
      t1.start();

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            node4.getCache(cacheName);
         }
      });
      t2.setName("CacheStarter-Cache4");
      t2.start();

      t1.join();
      t2.join();

      cache3 = node3.getCache(cacheName);
      cache4 = node4.getCache(cacheName);

      node3.waitForJoin(120000, cache1, cache2, cache3, cache4);
      node4.waitForJoin(120000, cache1, cache2, cache3, cache4);

      node3.verifyStateTransfer(new CacheVerifier(cache3));
      node4.verifyStateTransfer(new CacheVerifier(cache4));

      logTestEnd(m);
   }

   public void testSTWithThirdWritingNonTxCache(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      thirdWritingCacheTest(false);
      logTestEnd(m);
   }

   public void testSTWithThirdWritingTxCache(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      thirdWritingCacheTest(true);
      logTestEnd(m);
   }

   public void testSTWithWritingNonTxThread(Method m) throws Exception {
      TestResourceTracker.testThreadStarted(this);
      testCount++;
      logTestStart(m);
      writingThreadTest(false);
      logTestEnd(m);
   }

   public void testSTWithWritingTxThread(Method m) throws Exception {
      TestResourceTracker.testThreadStarted(this);
      testCount++;
      logTestStart(m);
      writingThreadTest(true);
      logTestEnd(m);
   }

   public void testInitialStateTransferAfterRestart(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      cache2.stop();
      cache2.start();

      verifyInitialData(cache2);
      logTestEnd(m);
   }

   private void logTestStart(Method m) {
      logTestLifecycle(m, "start");
   }

   private void logTestEnd(Method m) {
      logTestLifecycle(m, "end");
   }

   private void logTestLifecycle(Method m, String lifecycle) {
      log.infof("%s %s - %s", m.getName(), lifecycle, testCount);
   }

   private void thirdWritingCacheTest(boolean tx) throws Exception {
      Cache<Object, Object> cache1, cache2, cache3;
      cache1 = createCacheManager().getCache(cacheName);
      cache3 = createCacheManager().getCache(cacheName);
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache3);

      writeInitialData(cache1);

      // Delay the transient copy, so that we get a more thorough log test
      DelayTransfer value = new DelayTransfer();
      cache1.put("delay", value);
      value.enableDelay();

      WritingTask writingTask = new WritingTask(cache3, tx);
      Future<Integer> future = fork(writingTask);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);

      node2.waitForJoin(60000, cache1, cache2, cache3);

      writingTask.stop();
      int count = future.get(60, SECONDS);

      node2.verifyStateTransfer(new CacheVerifier(cache2));

      for (int c = 0; c < count; c++) {
         assertEquals(c, cache2.get("test" + c));
      }
   }

   protected void verifyInitialData(Cache<Object, Object> c) {
      Address address = c.getAdvancedCache().getRpcManager().getAddress();
      log.debugf("Checking values on cache " + address);
      assertEquals("Incorrect value for key " + A_B_NAME, JOE, c.get(A_B_NAME));
      assertEquals("Incorrect value for key " + A_B_AGE, TWENTY, c.get(A_B_AGE));
      assertEquals("Incorrect value for key " + A_C_NAME, BOB, c.get(A_C_NAME));
      assertEquals("Incorrect value for key " + A_C_AGE, FORTY, c.get(A_C_AGE));
   }

   protected void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
   }

   private void writingThreadTest(boolean tx) throws Exception {
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);

      writeInitialData(cache1);
      // Delay the transient copy, so that we get a more thorough log test
      DelayTransfer value = new DelayTransfer();
      cache1.put("delay", value);
      value.enableDelay();

      WritingTask writingTask = new WritingTask(cache1, tx);
      Future<Integer> future = fork(writingTask);
      verifyInitialData(cache1);

      JoiningNode node2 = new JoiningNode(createCacheManager());
      cache2 = node2.getCache(cacheName);
      node2.waitForJoin(60000, cache1, cache2);

      writingTask.stop();
      int count = future.get(60, SECONDS);

      verifyInitialData(cache1);
      node2.verifyStateTransfer(new CacheVerifier(cache2));

      for (int c = 0; c < count; c++) {
         assertEquals(c, cache2.get("test" + c));
      }
   }

   public class CacheVerifier implements Callable<Void> {

      private final Cache<Object, Object> cache;

      public CacheVerifier(Cache<Object, Object> cache) {
         this.cache = cache;
      }

      @Override
      public Void call() {
         verifyInitialData(cache);
         return null;
      }
   }

}
