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

package org.infinispan.tx.recovery;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import static org.testng.Assert.assertEquals;

public class RecoveryTestUtil {

   public static int count = 0;

   public static void commitTransaction(DummyTransaction dtx) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) dtx.firstEnlistedResource();
      xaResource.commit(xaResource.getLocalTransaction().getXid(), false);
   }

   public static void rollbackTransaction(DummyTransaction dtx) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) dtx.firstEnlistedResource();
      xaResource.commit(xaResource.getLocalTransaction().getXid(), false);
   }

   public static void prepareTransaction(DummyTransaction suspend1) {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) suspend1.firstEnlistedResource();
      try {
         xaResource.prepare(xaResource.getLocalTransaction().getXid());
      } catch (XAException e) {
         throw new RuntimeException(e);
      }
   }

   public static void assertPrepared(int count, DummyTransaction...tx) throws XAException {
      for (DummyTransaction dt : tx) {
         TransactionXaAdapter xaRes = (TransactionXaAdapter) dt.firstEnlistedResource();
         assertEquals(count, xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
      }
   }

   public static RecoveryManagerImpl rm(Cache cache) {
      return (RecoveryManagerImpl) TestingUtil.extractComponentRegistry(cache).getComponent(RecoveryManager.class);
   }

   public static DummyTransaction beginAndSuspendTx(Cache cache) {
      return beginAndSuspendTx(cache, "k" + count++);
   }

   public static DummyTransaction beginAndSuspendTx(Cache cache, Object key) {
      DummyTransactionManager dummyTm = (DummyTransactionManager) TestingUtil.getTransactionManager(cache);
      try {
         dummyTm.begin();
         cache.put(key, "v");
         return (DummyTransaction) dummyTm.suspend();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
