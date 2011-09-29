/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api.mvcc;

import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Test(groups = "functional", sequential = true, testName = "api.mvcc.LockPerEntryTest")
public class LockPerEntryTest extends SingleCacheManagerTest {   

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setUseLockStriping(false);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testLocksCleanedUp() {
      cache = cacheManager.getCache();
      cache.put("/a/b/c", "v");
      cache.put("/a/b/d", "v");
      assertNoLocks();
   }

   @Test (enabled = false)
   public void testLocksConcurrency() throws Exception {
      cache = cacheManager.getCache();
      final int NUM_THREADS = 10;
      final CountDownLatch l = new CountDownLatch(1);
      final int numLoops = 1000;
      final List<Exception> exceptions = new LinkedList<Exception>();

      Thread[] t = new Thread[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++)
         t[i] = new Thread() {
            public void run() {
               try {
                  l.await();
               }
               catch (Exception e) {
                  // ignore
               }
               for (int i = 0; i < numLoops; i++) {
                  try {
                     switch (i % 2) {
                        case 0:
                           cache.put("Key" + i, "v");
                           break;
                        case 1:
                           cache.remove("Key" + i);
                           break;
                     }
                  }
                  catch (Exception e) {
                     exceptions.add(e);
                  }
               }
            }
         };

      for (Thread th : t) th.start();
      l.countDown();
      for (Thread th : t) th.join();

      if (!exceptions.isEmpty()) throw exceptions.get(0);
      assertNoLocks();
   }

   private void assertNoLocks() {
      LockManager lm = TestingUtil.extractLockManager(cache);
      LockAssert.assertNoLocks(
            lm, TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class)
      );

      LockContainer lc = (LockContainer) TestingUtil.extractField(lm, "lockContainer");
      assert lc.size() == 0;
   }
}
