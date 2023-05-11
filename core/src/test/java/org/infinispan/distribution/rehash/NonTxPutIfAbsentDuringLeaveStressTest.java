package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPutIfAbsentDuringLeaveStressTest")
@CleanupAfterMethod
public class NonTxPutIfAbsentDuringLeaveStressTest extends MultipleCacheManagersTest {

   private static final int NUM_WRITERS = 4;
   private static final int NUM_ORIGINATORS = 2;
   private static final int NUM_KEYS = 100;

   @Override
   public Object[] factory() {
      return new Object[] {
            new NonTxPutIfAbsentDuringLeaveStressTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, false);

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   @Test(groups = "unstable", description = "ISPN-7682")
   public void testNodeLeavingDuringPutIfAbsent() throws Exception {
      ConcurrentMap<String, String> insertedValues = new ConcurrentHashMap<>();
      AtomicBoolean stop = new AtomicBoolean(false);

      Future[] futures = new Future[NUM_WRITERS];
      for (int i = 0; i < NUM_WRITERS; i++) {
         final int writerIndex = i;
         futures[i] = fork(new Callable() {
            @Override
            public Object call() throws Exception {
               while (!stop.get()) {
                  for (int j = 0; j < NUM_KEYS; j++) {
                     Cache<Object, Object> cache = cache(writerIndex % NUM_ORIGINATORS);
                     doPut(cache, "key_" + j, "value_" + j + "_" + writerIndex);
                  }
               }
               return null;
            }

            private void doPut(Cache<Object, Object> cache, String key, String value) {
               Object oldValue = cache.putIfAbsent(key, value);
               Object newValue = cache.get(key);
               if (oldValue == null) {
                  // succeeded
                  log.tracef("Successfully inserted value %s for key %s", value, key);
                  assertEquals(value, newValue);
                  String duplicateInsertedValue = insertedValues.putIfAbsent(key, value);
                  if (duplicateInsertedValue != null) {
                     // ISPN-4286: two concurrent putIfAbsent operations can both return null
                     assertEquals(value, duplicateInsertedValue);
                  }
               } else {
                  // failed
                  if (newValue == null) {
                     // ISPN-3918: cache.get(key) == null if another command succeeded but didn't finish
                     eventuallyEquals(oldValue, () -> cache.get(key));
                  } else {
                     assertEquals(oldValue, newValue);
                  }
               }
            }
         });
      }

      killMember(4);
      TestingUtil.waitForNoRebalance(caches());

      killMember(3);
      TestingUtil.waitForNoRebalance(caches());

      stop.set(true);

      for (int i = 0; i < NUM_WRITERS; i++) {
         futures[i].get(10, TimeUnit.SECONDS);
         for (int j = 0; j < NUM_KEYS; j++) {
            for (int k = 0; k < caches().size(); k++) {
               String key = "key_" + j;
               assertEquals(insertedValues.get(key), cache(k).get(key));
            }
         }
      }
   }
}
