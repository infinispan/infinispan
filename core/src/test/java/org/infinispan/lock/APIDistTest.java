package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.KeyAffinityServiceImpl;
import org.infinispan.affinity.KeyGenerator;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.infinispan.context.Flag.FAIL_SILENTLY;


@Test(testName = "lock.APITest", groups = "functional")
@CleanupAfterMethod
public class APIDistTest extends MultipleCacheManagersTest {
   EmbeddedCacheManager cm1, cm2;
   String key; // guaranteed to be mapped to cache2

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      cfg.setL1CacheEnabled(false); // no L1 enabled
      cfg.setLockAcquisitionTimeout(100);
      cfg.setNumOwners(1);
      cfg.setSyncCommitPhase(true);
      cfg.setSyncRollbackPhase(true);
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      cm1.getCache();
      Cache<String, String> c = cm2.getCache();

      // lets generate a key such that it is always mapped to cache2.
      KeyAffinityService<String> service = KeyAffinityServiceFactory.newKeyAffinityService(c,
              Executors.newCachedThreadPool(), new KeyGenerator<String>() {
                 final Random r = new Random();

                 @Override
                 public String getKey() {
                    return Integer.toHexString(r.nextInt(2000));
                 }
              }, 2);

      key = service.getKeyForAddress(c.getAdvancedCache().getRpcManager().getAddress());
   }

   public void testLockAndGet() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      tm(0).rollback();
   }

   public void testLockAndGetAndPut() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException, InterruptedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put(key, "v");

      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      assert "v".equals(cache2.get(key)) : "Could not find key " + key + " on cache2";

      tm(0).begin();
      cache1.getAdvancedCache().lock(key);
      assert "v".equals(cache1.get(key)) : "Could not find key " + key + " on cache1";
      String old = cache1.put(key, "new_value");
      assert "v".equals(old) : "Expected v, was " + old;
      tm(0).commit();

      String val;
      assert "new_value".equals(val = cache1.get(key)) : "Could not find key " + key + " on cache1: expected new_value, was " + val;
      assert "new_value".equals(val = cache2.get(key)) : "Could not find key " + key + " on cache2: expected new_value, was " + val;
   }

   public void testLockAndPutRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException, InterruptedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

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

   public void testLockAndRemoveRetval() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException, InterruptedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

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
