package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@Test(groups = "functional", testName = "statetransfer.StateTransferFunctionalTest", enabled = false)
public class StateTransferFunctionalTest extends MultipleCacheManagersTest {

   protected static final String ADDRESS_CLASSNAME = Address.class.getName();
   protected static final String PERSON_CLASSNAME = Person.class.getName();
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
      config = new Configuration();
      config.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      config.setSyncCommitPhase(true);
      config.setSyncReplTimeout(30000);
      config.setFetchInMemoryState(true);
      config.setUseLockStriping(false); // reduces the odd chance of a key collission and deadlock
      config.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
   }

   protected CacheManager createCacheManager() {
      CacheManager cm = addClusterEnabledCacheManager();
      cm.defineCache(cacheName, config.clone());
      return cm;
   }

   public static class DelayTransfer implements Serializable {
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

   public void testInitialStateTransfer() throws Exception {
      testCount++;
      log.info("testInitialStateTransfer start - " + testCount);
      Cache<Object, Object> cache1, cache2;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      cache2 = createCacheManager().getCache(cacheName);

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

      verifyInitialData(cache2);
      log.info("testInitialStateTransfer end - " + testCount);
   }

   public void testConcurrentStateTransfer() throws Exception {
      testCount++;
      log.info("testConcurrentStateTransfer start - " + testCount);
      Cache<Object, Object> cache1 = null, cache2 = null, cache3 = null, cache4 = null;
      cache1 = createCacheManager().getCache(cacheName);
      writeInitialData(cache1);

      cache2 = createCacheManager().getCache(cacheName);

      cache1.put("delay", new StateTransferFunctionalTest.DelayTransfer());

      // Pause to give caches time to see each other
      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
      verifyInitialData(cache2);

      final CacheManager cm3 = createCacheManager();
      final CacheManager cm4 = createCacheManager();

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
      verifyInitialData(cache3);
      verifyInitialData(cache4);
      log.info("testConcurrentStateTransfer end - " + testCount);
   }

   public void testSTWithThirdWritingNonTxCache() throws Exception {
      testCount++;
      log.info("testSTWithThirdWritingNonTxCache start - " + testCount);
      thirdWritingCacheTest(false);
      log.info("testSTWithThirdWritingNonTxCache end - " + testCount);
   }

   public void testSTWithThirdWritingTxCache() throws Exception {
      testCount++;
      log.info("testSTWithThirdWritingTxCache start - " + testCount);
      thirdWritingCacheTest(true);
      log.info("testSTWithThirdWritingTxCache end - " + testCount);
   }

   public void testSTWithWritingNonTxThread() throws Exception {
      testCount++;
      log.info("testSTWithWritingNonTxThread start - " + testCount);
      writingThreadTest(false);
      log.info("testSTWithWritingNonTxThread end - " + testCount);
   }

   public void testSTWithWritingTxThread() throws Exception {
      testCount++;
      log.info("testSTWithWritingTxThread start - " + testCount);
      writingThreadTest(true);
      log.info("testSTWithWritingTxThread end - " + testCount);
   }

   private void thirdWritingCacheTest(boolean tx) throws InterruptedException {
      Cache<Object, Object> cache1, cache2, cache3;
      cache1 = createCacheManager().getCache(cacheName);
      cache3 = createCacheManager().getCache(cacheName);

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

      for (int c = 0; c < count; c++)
         assert new Integer(c).equals(cache2.get("test" + c)) : "Entry under key [test" + c + "] was [" + cache2.get("test" + c) + "] but expected [" + c + "]";
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
}
