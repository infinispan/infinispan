/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.distribution;

import org.infinispan.Cache;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test that emulates transactions being started in a thread and then being
 * committed in a different thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.DistSyncTxCommitDiffThreadTest")
public class DistSyncTxCommitDiffThreadTest extends BaseDistFunctionalTest {

   public DistSyncTxCommitDiffThreadTest() {
      cacheName = this.getClass().getSimpleName();
      INIT_CLUSTER_SIZE = 2;
      sync = true;
      tx = true;
      l1CacheEnabled = false;
      numOwners = 1;
   }

   public void testCommitInDifferentThread(Method m) throws Exception {
      final String key = k(m), value = v(m);
      final Cache nonOwnerCache = getNonOwners(key, 1)[0];
      final Cache ownerCache = getOwners(key, 1)[0];
      final TransactionManager tmNonOwner = getTransactionManager(nonOwnerCache);
      final CountDownLatch commitLatch = new CountDownLatch(1);

      tmNonOwner.begin();
      final Transaction tx = tmNonOwner.getTransaction();
      Callable<Void> commitCallable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            tmNonOwner.resume(tx);
            commitLatch.await();
            tmNonOwner.commit();
            return null;
         }
      };
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future commitFuture = executor.submit(commitCallable);
      nonOwnerCache.put(key, value);
      commitLatch.countDown();
      commitFuture.get();

      Callable<Void> getCallable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            TransactionManager tmOwner = getTransactionManager(ownerCache);
            tmOwner.begin();
            assertEquals(value, ownerCache.get(key));
            tmOwner.commit();
            return null;
         }
      };

      Future getFuture = executor.submit(getCallable);
      getFuture.get();
      executor.shutdownNow();
   }

}
