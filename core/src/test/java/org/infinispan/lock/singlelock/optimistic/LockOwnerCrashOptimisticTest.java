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

package org.infinispan.lock.singlelock.optimistic;

import org.infinispan.config.Configuration;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.LockOwnerCrashOptimisticTest")
@CleanupAfterMethod
public class LockOwnerCrashOptimisticTest extends AbstractLockOwnerCrashTest {

   public LockOwnerCrashOptimisticTest() {
      super(Configuration.CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC, false);
   }

   DummyTransaction transaction;

   public void testLockOwnerCrashesBeforePrepare() throws Exception {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 1, 0)&& checkTxCount(2, 0, 0);
         }
      });

      killMember(2);
      assert caches().size() == 2;

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals(cache(0).get(k), "v");
      assertEquals(cache(1).get(k), "v");

      assertNotLocked(k);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit() throws Exception {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               transaction.runPrepare();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) &&  checkTxCount(1, 1, 0) &&  checkTxCount(2, 0, 1);
         }
      });

      killMember(2);
      assert caches().size() == 2;


      tm(1).begin();
      cache(1).put(k, "v3");
      try {
         tm(1).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         e.printStackTrace();
      }


      tm(0).begin();
      cache(0).put(k, "v2");
      try {
         tm(0).commit();
         fail("Exception expected as lock cannot be acquired on k=" + k);
      } catch (Exception e) {
         //expected
      }

      tm(1).resume(transaction);
      transaction.runPrepare();
   }
}
