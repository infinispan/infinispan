package org.infinispan.tx.lockreordering;

import org.infinispan.Cache;
import org.infinispan.commons.hash.MurmurHash2;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.lockreordering.LocalLockReorderingTest")
public class LocalLockReorderingTest extends SingleCacheManagerTest {

   private List<Integer> keys;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.transaction().cacheStopTimeout(1);
      keys = generateKeys();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testWithPut(Method m) throws Exception {
      runTest(StresserThread.PUT_PERFORMER, cache, cache, keys, getThreadName(m));

   }

   public void testWithRemove(Method m) throws InterruptedException {
      runTest(StresserThread.REMOVE_PERFORMER, cache, cache, keys, getThreadName(m));
   }

   public void testWithPutAll(Method m) throws InterruptedException {
      runTest(StresserThread.PUT_ALL_PERFORMER, cache, cache, keys, getThreadName(m));
   }

   public void testMixed(Method m) throws InterruptedException {
      runTest(StresserThread.MIXED_OPS_PERFORMER, cache, cache, keys, getThreadName(m));
   }

   static void runTest(StresserThread.OperationsPerformer ops, Cache c1, Cache c2, List<Integer> keys, String threadNamePrefix) throws InterruptedException {
      CyclicBarrier beforeCommit = new CyclicBarrier(2);

      StresserThread st1 = new StresserThread(c1, keys, "t1", ops, beforeCommit, threadNamePrefix + "-1");
      final ArrayList<Integer> reversedKeys = new ArrayList<Integer>(keys);
      Collections.reverse(reversedKeys);
      StresserThread st2 = new StresserThread(c2, reversedKeys, "t2", ops, beforeCommit, threadNamePrefix+"-2");

      st1.start();
      st2.start();

      st1.join();
      st2.join();

      assert !st1.isError();
      assert !st2.isError();
   }

   static List<Integer> generateKeys() {
      List<Integer> keys;
      /** this is what's used for inducing ordering */
      MurmurHash2 hashFunction = new MurmurHash2();
      keys = new ArrayList<Integer>(2);
      int count = 0;
      keys.add(count);
      while (keys.size() < 2) {
         final int hash = hashFunction.hash(++count);
         if (!keys.contains(hash)) keys.add(count);
      }
      return keys;
   }

   private String getThreadName(Method m) {
      return getClass().getSimpleName() + "." +m.getName();
   }

}
