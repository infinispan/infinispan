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
package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyXid;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.util.UUID;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(testName = "tx.TransactionXaAdapterTmIntegrationTest", groups = "unit", enabled = false, description = "Disabled due to instability - see ISPN-1123")
public class TransactionXaAdapterTmIntegrationTest {
   private Configuration configuration;
   private XaTransactionTable txTable;
   private GlobalTransaction globalTransaction;
   private LocalXaTransaction localTx;
   private TransactionXaAdapter xaAdapter;
   private DummyXid xid;
   private UUID uuid = UUID.randomUUID();

   @BeforeMethod
   public void setUp() {
      txTable = new XaTransactionTable();
      TransactionFactory gtf = new TransactionFactory();
      gtf.init(false, false, true);
      globalTransaction = gtf.newGlobalTransaction(null, false);
      localTx = new LocalXaTransaction(new DummyTransaction(null), globalTransaction, false, 1);
      xid = new DummyXid(uuid);
      localTx.setXid(xid);
      txTable.addLocalTransactionMapping(localTx);      

      configuration = new Configuration();
      TransactionCoordinator txCoordinator = new TransactionCoordinator();
      txCoordinator.init(null, null, null, null, configuration);
      xaAdapter = new TransactionXaAdapter(localTx, txTable, configuration, null, txCoordinator
      );
   }

   public void testPrepareOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testCommitOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.commit(xid, false);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testRollabckOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.rollback(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testPrepareTxMarkedForRollback() {
      localTx.markForRollback(true);
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XA_RBROLLBACK;
      }
   }

   public void testOnePhaseCommitConfigured() throws XAException {
      configuration.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);//this would force 1pc
      assert XAResource.XA_OK == xaAdapter.prepare(xid);
   }

   public void test1PcAndNonExistentXid() {
      configuration.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);
      try {
         DummyXid doesNotExists = new DummyXid(uuid);
         xaAdapter.commit(doesNotExists, false);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void test1PcAndNonExistentXid2() {
      configuration.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      try {
         DummyXid doesNotExists = new DummyXid(uuid);
         xaAdapter.commit(doesNotExists, true);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }
}
