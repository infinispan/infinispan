package org.infinispan.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;


@Test(testName = "lock.APIDistTest", groups = "functional")
@CleanupAfterMethod
public class APIDistTest extends MultipleCacheManagersTest {
   MagicKey key; // guaranteed to be mapped to cache2

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, createConfig(), 2);
      waitForClusterToForm();
      key = new MagicKey("Key mapped to Cache2", cache(1));
   }

   protected ConfigurationBuilder createConfig() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg
            .transaction().lockingMode(LockingMode.PESSIMISTIC)
            .clustering().l1().disable().hash().numOwners(1)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      return cfg;
   }

   public void testLockAndGet() throws SystemException, NotSupportedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      assertEquals(cache2.get(key), "v", "Could not find key " + key + " on cache2");

      tm(0).begin();
      log.trace("About to lock");
      cache1.getAdvancedCache().lock(key);
      log.trace("About to get");
      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      tm(0).rollback();
   }

   public void testLockAndGetAndPut() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      assertEquals(cache2.get(key), "v", "Could not find key " + key + " on cache2");

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      String old = cache1.put(key, "new_value");
      assertEquals(old, "v", "Expected v, was " + old);
      log.trace("Before commit!");
      tm(0).commit();

      assertEquals("new_value", cache1.get(key), "Could not find key " + key + " on cache 1.");
      assertEquals("new_value", cache2.get(key), "Could not find key " + key + " on cache 2.");
   }

   public void testLockAndPutRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      assertEquals(cache2.get(key), "v", "Could not find key " + key + " on cache2");

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      String old = cache1.put(key, "new_value");
      assertEquals(old, "v", "Expected v, was " + old);
      tm(0).commit();

      assertEquals("new_value", cache1.get(key), "Could not find key " + key + " on cache 1.");
      assertEquals("new_value", cache2.get(key), "Could not find key " + key + " on cache 2.");
   }

   public void testLockAndRemoveRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assertEquals(cache1.get(key), "v", "Could not find key " + key + " on cache1");
      assertEquals(cache2.get(key), "v", "Could not find key " + key + " on cache2");

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      String old = cache1.remove(key);
      assertEquals(old, "v", "Expected v, was " + old);
      tm(0).commit();

      assertNull(cache1.get(key), "Could not find key " + key + " on cache 1.");
      assertNull(cache2.get(key), "Could not find key " + key + " on cache 2.");
   }
}
