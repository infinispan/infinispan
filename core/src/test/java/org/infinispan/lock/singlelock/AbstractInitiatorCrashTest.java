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

package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractInitiatorCrashTest extends AbstractCrashTest {

   public AbstractInitiatorCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      super(cacheMode, lockingMode, useSynchronization);
   }

   public void testInitiatorCrashesBeforeReleasingLock() throws Exception {
      final CountDownLatch releaseLocksLatch = new CountDownLatch(1);

      prepareCache(releaseLocksLatch);

      Object k = getKeyForCache(2);
      beginAndCommitTx(k, 1);
      releaseLocksLatch.await();

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 1);

      assertNotLocked(cache(0), k);
      assertNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      killMember(1);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
      assertNotLocked(k);
   }

   public void testInitiatorNodeCrashesBeforeCommit() throws Exception {

      Object k = getKeyForCache(2);
      
      tm(1).begin();
      cache(1).put(k,"v");
      final DummyTransaction transaction = (DummyTransaction) tm(1).getTransaction();
      transaction.runPrepare();
      tm(1).suspend();

      assertNotLocked(cache(0), k);
      assertNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      checkTxCount(0, 0, 1);
      checkTxCount(1, 1, 0);
      checkTxCount(2, 0, 1);

      killMember(1);

      assertNotLocked(k);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }
}
