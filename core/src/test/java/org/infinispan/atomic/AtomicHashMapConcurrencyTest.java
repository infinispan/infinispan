package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Tester class for AtomicMapCache.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "atomic.AtomicHashMapConcurrencyTest")
public class AtomicHashMapConcurrencyTest extends AbstractInfinispanTest {

   public static final String KEY = "key";
   Cache<String, Object> cache;
   TransactionManager tm;
   private CacheManager cm;

   enum Operation {
      PUT,
      COMMIT,
      READ
   }

   @BeforeMethod
   @SuppressWarnings("unchecked")
   public void setUp() {
      Configuration c = new Configuration();
      c.setLockAcquisitionTimeout(500);
      // these 2 need to be set to use the AtomicMapCache
      c.setInvocationBatchingEnabled(true);
      cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() {
      try {
         tm.rollback();
      } catch (Exception e) {
      }
      TestingUtil.killCacheManagers(cm);
   }

   public void testConcurrentCreate() throws Exception {
      tm.begin();
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, KEY);
      map.put("blah", "blah");
      OtherThread ot = new OtherThread();
      ot.start();
      Object response = ot.response.take();
      assert response instanceof TimeoutException;
   }

   public void testConcurrentModifications() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm.begin();
      atomicMap.put(1, "");
      OtherThread ot = new OtherThread();
      ot.start();
      ot.toExecute.put(Operation.PUT);
      Object response = ot.response.take();
      assert response instanceof TimeoutException;
   }

   public void testReadAfterTxStarted() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      cache.putIfAbsent(KEY, AtomicHashMap.newInstance());
      atomicMap.put(1, "existing");
      tm.begin();
      atomicMap.put(1, "newVal");
      OtherThread ot = new OtherThread();
      ot.start();
      ot.toExecute.put(Operation.READ);
      Object response = ot.response.take();
      assert response.equals("existing");
      tm.commit();
      assert atomicMap.get(1).equals("newVal");
      ot.toExecute.put(Operation.READ);
      response = ot.response.take();
      assert response.equals("newVal");
   }

   public class OtherThread extends Thread {

      public OtherThread() {
         super("OtherThread");
      }

      BlockingQueue<Object> response = new ArrayBlockingQueue<Object>(1);

      BlockingQueue<Operation> toExecute = new ArrayBlockingQueue<Operation>(1);

      @Override
      public void run() {
         try {
            tm.begin();
            AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
            if (cache.get(KEY) == null) cache.putIfAbsent(KEY, AtomicHashMap.newInstance());
            boolean notCommited = true;
            while (notCommited) {
               Operation op = toExecute.take();
               switch (op) {
                  case PUT: {
                     atomicMap.put(1, "val");
                     response.put(new Object());
                     break;
                  }
                  case READ: {
                     String val = atomicMap.get(1);
                     response.put(String.valueOf(val));
                     break;
                  }
                  case COMMIT: {
                     tm.commit();
                     response.put(new Object());
                     notCommited = false;
                     break;
                  }
               }
            }
         } catch (Exception e) {
            try {
               response.put(e);
            } catch (InterruptedException e1) {
               e1.printStackTrace();
            }
            e.printStackTrace();
         }
      }
   }
}
