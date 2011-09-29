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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Arrays;

import static org.infinispan.context.Flag.FAIL_SILENTLY;


@Test(testName = "lock.APITest", groups = "functional")
@CleanupAfterMethod
public class APITest extends MultipleCacheManagersTest {
   EmbeddedCacheManager cm1, cm2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      cfg.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC).cacheStopTimeout(0);

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
      tm(1).suspend();

      tm(0).begin();
      cache1.getAdvancedCache().lock("k");
      tm(0).rollback();
   }

   public void testSilentLockFailure() throws SystemException, NotSupportedException {
      Cache<String, String> cache1 = cache(0), cache2 = cache(1);

      cache1.put("k", "v");
      tm(1).begin();
      cache2.put("k", "v2");
      tm(1).suspend();

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
      tm(1).suspend();

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
