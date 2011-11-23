package org.infinispan.tx.lockreordering;

import static org.infinispan.tx.lockreordering.LocalLockReorderingTest.runTest;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.util.hash.MurmurHash3;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.lockreordering.DistLockReorderingTest")
public class DistLockReorderingTest extends MultipleCacheManagersTest {

   List keys;
   protected Configuration.CacheMode cacheMode = Configuration.CacheMode.DIST_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration c = getDefaultClusteredConfig(cacheMode, true);
      c.fluent().transaction().cacheStopTimeout(1);
      c.fluent().transaction().locking().lockAcquisitionTimeout(30000L);//timeouts are possible otherwise
      createCluster(c, 2);
      waitForClusterToForm();
      buildKeys();
   }

   void buildKeys() {
      int node = (int) (System.nanoTime() % 2);
      /** this is what's used for inducing ordering */
      MurmurHash3 hashFunction = new MurmurHash3();
      keys = new ArrayList<Integer>(2);
      final Object firstKey = getKeyForCache(node);
      keys.add(firstKey);
      while (keys.size() < 2) {
         final Object keyForCache = getKeyForCache(node);
         final int hash = hashFunction.hash(keyForCache);
         if (hash != hashFunction.hash(firstKey)) keys.add(keyForCache);
      }
   }

   public void testWithPut(Method m) throws Exception {
      runTest(StresserThread.PUT_PERFORMER, cache(0), cache(1), keys, getThreadPrefix(m));

   }

   public void testWithRemove(Method m) throws InterruptedException {
      runTest(StresserThread.REMOVE_PERFORMER, cache(0), cache(1), keys, getThreadPrefix(m));
   }

   public void testWithPutAll(Method m) throws InterruptedException {
      runTest(StresserThread.PUT_ALL_PERFORMER, cache(0), cache(1), keys, getThreadPrefix(m));
   }

   public void testMixed(Method m) throws InterruptedException {
      runTest(StresserThread.MIXED_OPS_PERFORMER, cache(0), cache(1), keys, getThreadPrefix(m));
   }

   private String getThreadPrefix(Method m) {
      return getClass().getSimpleName() + "." + m.getName();
   }
}
