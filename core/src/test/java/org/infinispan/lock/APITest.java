/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lock;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.locking.AbstractLockingInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.infinispan.context.Flag.FAIL_SILENTLY;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.Assert.assertTrue;


@Test(testName = "lock.APITest", groups = "functional")
@CleanupAfterMethod
public class APITest extends MultipleCacheManagersTest {
   private EmbeddedCacheManager cm1, cm2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg.transaction().lockingMode(LockingMode.PESSIMISTIC)
            .cacheStopTimeout(0)
            .locking().lockAcquisitionTimeout(100);

      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      cm1.getCache();
      cm2.getCache();
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
      final ExecutorService e = Executors.newCachedThreadPool();
      final CountDownLatch waitLatch = new CountDownLatch(1);
      final CountDownLatch continueLatch = new CountDownLatch(1);
      cache.put(1, "v1");

      Future<Void> f1 = e.submit(new Callable<Void>() {
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


      Future<Void> f2 = e.submit(new Callable<Void>() {
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
      Transaction t = tm(1).suspend();

      tm(0).begin();
      assert !cache1.getAdvancedCache().withFlags(FAIL_SILENTLY).lock(Arrays.asList("k1", "k2", "k3"));
      tm(0).rollback();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testLockOnNonTransactionalCache() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createLocalCacheManager(false)) {
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
