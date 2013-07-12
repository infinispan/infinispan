package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;


@Test(testName = "lock.APIDistTest", groups = "functional")
@CleanupAfterMethod
public class APIDistTest extends MultipleCacheManagersTest {
   EmbeddedCacheManager cm1, cm2;
   MagicKey key; // guaranteed to be mapped to cache2

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = createConfig();
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      cm1.getCache();
      waitForClusterToForm();
      key = new MagicKey("Key mapped to Cache2", cm2.getCache());
   }

   protected ConfigurationBuilder createConfig() {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cfg
         .transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(true).syncRollbackPhase(true)
         .clustering().l1().disable().hash().numOwners(1)
         .locking().lockAcquisitionTimeout(100);
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

      String val;
      assert "new_value".equals(val = cache1.get(key)) : "Could not find key " + key + " on cache1: expected new_value, was " + val;
      assert "new_value".equals(val = cache2.get(key)) : "Could not find key " + key + " on cache2: expected new_value, was " + val;
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

      String val;
      assert "new_value".equals(val = cache1.get(key)) : "Could not find key " + key + " on cache1: expected new_value, was " + val;
      assert "new_value".equals(val = cache2.get(key)) : "Could not find key " + key + " on cache2: expected new_value, was " + val;
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

      String val;
      assert (null == (val = cache1.get(key))) : "Could not find key " + key + " on cache1: expected null, was " + val;
      assert (null == (val = cache2.get(key))) : "Could not find key " + key + " on cache2: expected null, was " + val;
   }
}
