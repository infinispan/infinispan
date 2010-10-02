package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.infinispan.context.Flag.FAIL_SILENTLY;


@Test(testName = "lock.APITest", groups = "functional")
@CleanupAfterMethod
public class APITest extends MultipleCacheManagersTest {
   EmbeddedCacheManager cm1, cm2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      cfg.setLockAcquisitionTimeout(100);
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      cm1.getCache();
      cm2.getCache();
   }

   public void testLockSuccess() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0);

      cache1.put("k", "v");
      tm(0).begin();
      assert cache1.getAdvancedCache().lock("k");
      tm(0).rollback();
   }

   @Test (expectedExceptions = TimeoutException.class)
   public void testLockFailure() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k", "v");
      tm(1).begin();
      cache2.put("k", "v2");
      Transaction t = tm(1).suspend();

      tm(0).begin();
      cache1.getAdvancedCache().lock("k");
      tm(0).rollback();
   }

   public void testSilentLockFailure() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k", "v");
      tm(1).begin();
      cache2.put("k", "v2");
      Transaction t = tm(1).suspend();

      tm(0).begin();
      assert !cache1.getAdvancedCache().withFlags(FAIL_SILENTLY).lock("k");
      tm(0).rollback();
   }

   public void testMultiLockSuccess() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(0).begin();
      assert cache1.getAdvancedCache().lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   @Test (expectedExceptions = TimeoutException.class)   
   public void testMultiLockFailure() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(1).begin();
      cache2.put("k3", "v2");
      Transaction t = tm(1).suspend();

      tm(0).begin();
      cache1.getAdvancedCache().lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   public void testSilentMultiLockFailure() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(1).begin();
      cache2.put("k3", "v2");
      Transaction t = tm(1).suspend();

      tm(0).begin();
      assert !cache1.getAdvancedCache().withFlags(FAIL_SILENTLY).lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }
}
