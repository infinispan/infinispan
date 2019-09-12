package org.infinispan.lock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

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

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      log.trace("About to lock");
      cache1.getAdvancedCache().lock(key);
      log.trace("About to get");
      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      tm(0).rollback();
   }

   public void testLockAndGetAndPut() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      String old = cache1.put(key, "new_value");
      assert "v".equals(old) : "Expected v, was " + old;
      log.trace("Before commit!");
      tm(0).commit();

      assertEquals("Could not find key " + key + " on cache 1.", "new_value", cache1.get(key));
      assertEquals("Could not find key " + key + " on cache 2.", "new_value", cache2.get(key));
   }

   public void testLockAndPutRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      String old = cache1.put(key, "new_value");
      assert "v".equals(old) : "Expected v, was " + old;
      tm(0).commit();

      assertEquals("Could not find key " + key + " on cache 1.", "new_value", cache1.get(key));
      assertEquals("Could not find key " + key + " on cache 2.", "new_value", cache2.get(key));
   }

   public void testLockAndRemoveRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      Cache<MagicKey, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      String old = cache1.remove(key);
      assert "v".equals(old) : "Expected v, was " + old;
      tm(0).commit();

      assertNull("Could not find key " + key + " on cache 1.", cache1.get(key));
      assertNull("Could not find key " + key + " on cache 2.", cache2.get(key));
   }
}
