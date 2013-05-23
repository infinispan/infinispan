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

package org.infinispan.lock.singlelock.pessimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.pessimistic.LockOwnerCrashPessimisticTest")
@CleanupAfterMethod
public class LockOwnerCrashPessimisticTest extends AbstractLockOwnerCrashTest {

   public LockOwnerCrashPessimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.PESSIMISTIC, false);
   }

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

      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });

      killMember(2);
      assert caches().size() == 2;

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals(cache(0).get(k), "v");
      assertEquals(cache(1).get(k), "v");

      assertNotLocked(k);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void testLockOwnerCrashesBeforePrepareAndLockIsStillHeld() throws Exception {
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
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });

      killMember(2);
      assert caches().size() == 2;

      tm(0).begin();
      try {
         cache(0).put(k, "v1");
         assert false;
      } catch (Exception e) {
         tm(0).rollback();
      }

      tm(1).resume(transaction);
      tm(1).commit();

      assertEquals(cache(0).get(k), "v");
      assertEquals(cache(1).get(k), "v");

      assertNotLocked(k);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit1() throws Exception {
      testCrashBeforeCommit(true);
   }

   public void lockOwnerCrasherBetweenPrepareAndCommit2() throws Exception {
      testCrashBeforeCommit(false);
   }

   private void testCrashBeforeCommit(final boolean crashBeforePrepare) throws NotSupportedException, SystemException, InvalidTransactionException, HeuristicMixedException, RollbackException, HeuristicRollbackException {
      final Object k = getKeyForCache(2);
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(1).begin();
               cache(1).put(k, "v");
               transaction = (DummyTransaction) tm(1).getTransaction();
               if (!crashBeforePrepare) {
                  transaction.runPrepare();
               }
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);


      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 1) && checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1);
         }
      });

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, k) && !checkLocked(1, k) && checkLocked(2, k);
         }
      });


      killMember(2);
      assert caches().size() == 2;


      tm(1).begin();
      try {
         cache(1).put(k, "v2");
         assert false : "Exception expected as lock cannot be acquired on k=" + k;
      } catch (Exception e) {
         tm(1).rollback();
      }

      tm(0).begin();
      try {
         cache(0).put(k, "v3");
         assert false : "Exception expected as lock cannot be acquired on k=" + k;
      } catch (Exception e) {
         tm(0).rollback();
      }


      tm(1).resume(transaction);
      if (!crashBeforePrepare) {
         transaction.runCommitTx();
      } else {
         tm(1).commit();
      }
      assert cache(0).get(k).equals("v");
      assert cache(1).get(k).equals("v");
      assertNotLocked(k);
   }
}
