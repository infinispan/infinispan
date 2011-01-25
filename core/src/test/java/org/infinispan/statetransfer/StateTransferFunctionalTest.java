package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

@Test(groups = "functional", testName = "statetransfer.StateTransferFunctionalTest", enabled = true)
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

   Configuration config;
   protected static String cacheName = "nbst";

   private volatile int testCount = 0;

   private static final Log log = LogFactory.getLog(StateTransferFunctionalTest.class);

   public StateTransferFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      // This impl only really sets up a configuration for use later.
      config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      config.setSyncReplTimeout(30000);
      config.setFetchInMemoryState(true);
      config.setUseLockStriping(false); // reduces the odd chance of a key collision and deadlock
   }

   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, config.clone());
      return cm;
   }

   public static class DelayTransfer implements Serializable {

      private static final long serialVersionUID = 6361429803359702822L;
      
      private transient int count;

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
         in.defaultReadObject();
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();

         // RPC is first serialization, ST is second
         if (count++ == 0)
            return;

         try {
            // This sleep is not required for the test to function,
            // however it improves the possibility of finding errors
            // (since it keeps the tx log going)
            Thread.sleep(2000);
         }
         catch (InterruptedException e) {
         }
      }

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
//               if (c == 1000) {
//                  if (tx) tm.begin();
//                  for (int i = 0; i < 1000; i++) cache.remove("test" + i);
//                  if (tx) tm.commit();
//                  c = 0;
//               } else {
               if (tx) tm.begin();
               cache.put("test" + c, c++);
               if (tx) tm.commit();
//               }
            }
            catch (Exception e) {
//               e.printStackTrace();
//               log.error(e);
               stopThread();
            }
         }
         result = c;
      }

      public void stopThread() {
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

      EmbeddedCacheManager cm2 = createCacheManager();
      MergedViewListener mv2 = new MergedViewListener();
      cm2.addListener(mv2);
      cache2 = cm2.getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

      if (!mv2.merged) {
         verifyInitialData(cache2);
      }

      logTestEnd(m);
   }

   public void testInitialStateTransferCacheNotPresent(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2;
      EmbeddedCacheManager cacheManager1 = createCacheManager();
      cache1 = cacheManager1.getCache(cacheName);
      writeInitialData(cache1);
      cache2 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
      verifyInitialData(cache2);

      cacheManager1.defineConfiguration("otherCache", config.clone());
      cacheManager1.getCache("otherCache");
      logTestEnd(m);
   }

   public void testConcurrentStateTransfer(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      Cache<Object, Object> cache1, cache2, cache3, cache4;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      cache2 = createCacheManager().getCache(cacheName);

      cache1.put("delay", new StateTransferFunctionalTest.DelayTransfer());

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
      verifyInitialData(cache2);

      final EmbeddedCacheManager cm3 = createCacheManager();
      MergedViewListener l3 = new MergedViewListener();
      cm3.addListener(l3);
      final EmbeddedCacheManager cm4 = createCacheManager();
      MergedViewListener l4 = new MergedViewListener();
      cm4.addListener(l4);

      Thread t1 = new Thread(new Runnable() {
         public void run() {
            cm3.getCache(cacheName);
         }
      });
      t1.setName("CacheStarter-Cache3");
      t1.start();

      Thread t2 = new Thread(new Runnable() {
         public void run() {
            cm4.getCache(cacheName);
         }
      });
      t2.setName("CacheStarter-Cache4");
      t2.start();

      t1.join();
      t2.join();

      cache3 = cm3.getCache(cacheName);
      cache4 = cm4.getCache(cacheName);

      TestingUtil.blockUntilViewsReceived(120000, cache1, cache2, cache3, cache4);
      //in the case of merges no state transfer happens so no point in verifying whether state was actually migrated
      if (!l3.merged) {
         verifyInitialData(cache3);
      }
      if (!l4.merged) {
         verifyInitialData(cache4);
      }
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

   @Test (timeOut = 120000)
   public void testSTWithWritingNonTxThread(Method m) throws Exception {
      testCount++;
      logTestStart(m);
      writingThreadTest(false);
      logTestEnd(m);
   }

   @Test (timeOut = 120000)
   public void testSTWithWritingTxThread(Method m) throws Exception {
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

      cache2 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

      verifyInitialData(cache2);

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
      log.info("{0} {1} - {2}", m.getName(), lifecycle, testCount);
   }

   private void thirdWritingCacheTest(boolean tx) throws InterruptedException {
      Cache<Object, Object> cache1, cache2, cache3;
      cache1 = createCacheManager().getCache(cacheName);
      cache3 = createCacheManager().getCache(cacheName);
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache3);

      writeInitialData(cache1);

      // Delay the transient copy, so that we get a more thorough log test
      cache1.put("delay", new DelayTransfer());

      WritingThread writerThread = new WritingThread(cache3, tx);
      writerThread.start();

      cache2 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2, cache3);

      writerThread.stopThread();
      writerThread.join();

      verifyInitialData(cache2);

      int count = writerThread.result();

      for (int c = 0; c < count; c++) {
         Object o = cache2.get("test" + c);
         assert new Integer(c).equals(o) : "Entry under key [test" + c + "] was [" + cache2.get("test" + c) + "] but expected [" + c + "]";
      }
   }

   protected void verifyInitialData(Cache<Object, Object> c) {
      assert JOE.equals(c.get(A_B_NAME)) : "Incorrect value for key " + A_B_NAME;
      assert TWENTY.equals(c.get(A_B_AGE)) : "Incorrect value for key " + A_B_AGE;
      assert BOB.equals(c.get(A_C_NAME)) : "Incorrect value for key " + A_C_NAME;
      assert FORTY.equals(c.get(A_C_AGE)) : "Incorrect value for key " + A_C_AGE;
   }

   protected void writeInitialData(final Cache<Object, Object> c) {
      c.put(A_B_NAME, JOE);
      c.put(A_B_AGE, TWENTY);
      c.put(A_C_NAME, BOB);
      c.put(A_C_AGE, FORTY);
   }

   private void writingThreadTest(boolean tx) throws InterruptedException {
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);

      writeInitialData(cache1);
      // Delay the transient copy, so that we get a more thorough log test
      cache1.put("delay", new DelayTransfer());

      WritingThread writerThread = new WritingThread(cache1, tx);
      writerThread.start();
      verifyInitialData(cache1);
      cache2 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

      writerThread.stopThread();
      writerThread.join();

      verifyInitialData(cache1);
      verifyInitialData(cache2);

      int count = writerThread.result();

      for (int c = 0; c < count; c++)
         assert new Integer(c).equals(cache2.get("test" + c)) : "Entry under key [test" + c + "] was [" + cache2.get("test" + c) + "] but expected [" + c + "]";
   }

   @Listener
   public static class MergedViewListener {

      public volatile boolean merged;

      @Merged
      public void mergedView(MergeEvent me) {
         log.info("View merged received {0}", me);
         merged = true;
      }
   }
}
