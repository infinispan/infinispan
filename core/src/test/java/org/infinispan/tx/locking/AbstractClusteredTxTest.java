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

package org.infinispan.tx.locking;

import java.util.Collections;
import java.util.Map;
import javax.transaction.HeuristicMixedException;
import javax.transaction.SystemException;

import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractClusteredTxTest extends MultipleCacheManagersTest {
   
   Object k;

   public void testPut() throws Exception {
      tm(0).begin();
      cache(0).put(k, "v");
      assertLocking();
   }

   public void testRemove() throws Exception {
      tm(0).begin();
      cache(0).remove(k);
      assertLocking();
   }

   public void testReplace() throws Exception {
      tm(0).begin();
      cache(0).replace(k, "v1");
      assertLockingNoChanges();

      // if the key doesn't exist, replace is a no-op, so it shouldn't acquire locks
      cache(0).put(k, "v1");

      tm(0).begin();
      cache(0).replace(k, "v2");
      assertLocking();
   }

   public void testClear() throws Exception {
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).clear();
      assertLocking();
   }

   public void testPutAll() throws Exception {
      Map m = Collections.singletonMap(k, "v");
      tm(0).begin();
      cache(0).putAll(m);
      assertLocking();
   }

   public void testRollbackOnPrimaryOwner() throws Exception {
      testRollback(0);
   }

   public void testRollbackOnBackupOwner() throws Exception {
      testRollback(1);
   }

   private void testRollback(int executeOn) throws Exception {
      tm(executeOn).begin();
      cache(executeOn).put(k, "v");
      assertLockingOnRollback();
      assertNoTransactions();
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));
   }

   protected void commit() {
      DummyTransactionManager dtm = (DummyTransactionManager) tm(0);
      try {
         dtm.getTransaction().runCommitTx();
      } catch (HeuristicMixedException e) {
         throw new RuntimeException(e);
      }
   }

   protected void prepare() {
      DummyTransactionManager dtm = (DummyTransactionManager) tm(0);
      try {
         dtm.getTransaction().runPrepare();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected void rollback() {
      DummyTransactionManager dtm = (DummyTransactionManager) tm(0);
      try {
         dtm.getTransaction().rollback();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected abstract void assertLocking();

   protected abstract void assertLockingNoChanges();

   protected abstract void assertLockingOnRollback();
}
