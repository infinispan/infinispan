/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests DummyTransactionManager commit issues when prepare fails.
 *
 * @author anistor@redhat.com
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.DummyTxTest")
public class DummyTxTest extends SingleCacheManagerTest {

   protected final Log log = LogFactory.getLog(getClass());

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);  // also try this test with 'true' so you can tell the difference between DummyTransactionManager and JBoss TM

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().lockAcquisitionTimeout(200).writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ);

      cm.defineConfiguration("test", cb.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testConcurrentRemove() throws Exception {
      cache.put("k1", "v1");

      // multiple threads will try to remove "k1" and commit. we expect only one to succeed

      final int numThreads = 5;
      final AtomicInteger removed = new AtomicInteger();
      final AtomicInteger rolledBack = new AtomicInteger();

      final CountDownLatch latch = new CountDownLatch(1);
      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < numThreads; i++) {
         threads[i] = new Thread("DummyTxTest.Remover-" + i) {
            public void run() {
               try {
                  latch.await();

                  tm().begin();
                  try {
                     boolean success = cache.remove("k1", "v1");
                     TestingUtil.sleepRandom(200);
                     tm().commit();

                     if (success) {
                        removed.incrementAndGet();
                     }
                  } catch (Throwable e) {
                     if (e instanceof RollbackException) {
                        rolledBack.incrementAndGet();
                     }

                     // the TX is most likely rolled back already, but we attempt a rollback just in case it isn't
                     if (tm().getTransaction() != null) {
                        try {
                           tm().rollback();
                        } catch (SystemException e1) {
                           log.error("Failed to rollback", e1);
                        }
                     }
                     throw e;
                  }
               } catch (Throwable e) {
                  log.error(e);
               }
            }
         };
         threads[i].start();
      }

      latch.countDown();
      for (Thread t : threads) {
         t.join();
      }

      log.trace("removed= " + removed.get());
      log.trace("rolledBack= " + rolledBack.get());

      assertFalse(cache.containsKey("k1"));
      assertEquals(1, removed.get());
      assertEquals(numThreads - 1, rolledBack.get());
   }
}
