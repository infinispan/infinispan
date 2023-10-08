package org.infinispan.lock;

import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.TimeoutException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.locking.AbstractLockingInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.testng.annotations.Test;

import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;


@Test(testName = "lock.APITest", groups = "functional")
@CleanupAfterMethod
public class APITest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg.clustering().hash().numSegments(1)
            .consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
      cfg.transaction().lockingMode(LockingMode.PESSIMISTIC)
            .cacheStopTimeout(0)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createCluster(ReplicatedControlledConsistentHashFactory.SCI.INSTANCE, cfg, 2);
   }

   public void testProperties() {
      Properties p = new Properties();

      Object v = new Object();
      p.put("bla", v);
      assertEquals(v, p.get("bla"));
      System.out.println(p.get("bla"));
   }

   public void testLockSuccess() throws Exception {
      Cache<String, String> cache1 = cache(0);

      cache1.put("k", "v");
      tm(0).begin();
      assert cache1.getAdvancedCache().lock("k");
      tm(0).rollback();
   }

   @Test (expectedExceptions = TimeoutException.class)
   public void testLockFailure() throws Exception {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k", "v");
      tm(1).begin();
      cache2.put("k", "v2");
      tm(1).suspend();

      tm(0).begin();
      cache1.getAdvancedCache().lock("k");
      tm(0).rollback();
   }

   public void testSilentLockFailure() throws Exception {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k", "v");
      tm(1).begin();
      cache2.put("k", "v2");
      tm(1).suspend();

      tm(0).begin();
      assert !cache1.getAdvancedCache().withFlags(FAIL_SILENTLY).lock("k");
      tm(0).rollback();
   }

   public void testSilentLockFailureAffectsPostOperations() throws Exception {
      final Cache<Integer, String> cache = cache(0);
      final TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      final CountDownLatch waitLatch = new CountDownLatch(1);
      final CountDownLatch continueLatch = new CountDownLatch(1);
      cache.put(1, "v1");

      Future<Void> f1 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            tm.begin();
            try {
               cache.put(1, "v2");
               waitLatch.countDown();
               continueLatch.await();
            } catch (Exception e) {
               tm.setRollbackOnly();
               throw e;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
               else tm.rollback();
            }
            return null;
         }
      });


      Future<Void> f2 = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitLatch.await();
            tm.begin();
            try {
               AdvancedCache<Integer, String> silentCache = cache.getAdvancedCache().withFlags(
                     Flag.FAIL_SILENTLY, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
               silentCache.put(1, "v3");
               assert !silentCache.lock(1);
               String object = cache.get(1);
               assert "v1".equals(object) : "Expected v1 but got " + object;
               cache.get(1);
            } catch (Exception e) {
               tm.setRollbackOnly();
               throw e;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
               else tm.rollback();

               continueLatch.countDown();
            }
            return null;
         }
      });

      f1.get();
      f2.get();
   }

   public void testMultiLockSuccess() throws Exception {
      Cache<String, String> cache1 = cache(0);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(0).begin();
      assert cache1.getAdvancedCache().lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   @Test (expectedExceptions = TimeoutException.class)
   public void testMultiLockFailure() throws Exception {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(1).begin();
      cache2.put("k3", "v2");
      tm(1).suspend();

      tm(0).begin();
      cache1.getAdvancedCache().lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   public void testSilentMultiLockFailure() throws Exception {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k1", "v");
      cache1.put("k2", "v");
      cache1.put("k3", "v");

      tm(1).begin();
      cache2.put("k3", "v2");
      tm(1).suspend();

      tm(0).begin();
      assert !cache1.getAdvancedCache().withFlags(FAIL_SILENTLY).lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testLockOnNonTransactionalCache() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            cm.getCache().getAdvancedCache().lock("k");
         }
      });
   }

   public void testLockingInterceptorType() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            AbstractLockingInterceptor lockingInterceptor = TestingUtil.findInterceptor(
                  cm.getCache(), AbstractLockingInterceptor.class);
            assertTrue(lockingInterceptor instanceof NonTransactionalLockingInterceptor);
         }
      });
   }

}
