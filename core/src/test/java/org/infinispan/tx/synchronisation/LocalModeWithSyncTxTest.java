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

package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.LocalModeTxTest;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronization.LocalModeWithSyncTxTest")
public class LocalModeWithSyncTxTest extends LocalModeTxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      Configuration config = getDefaultStandaloneConfig(true);
      config.fluent().transaction().transactionManagerLookupClass(DummyTransactionManagerLookup.class);
      config.fluent().transaction().useSynchronization(true);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   public void testSyncRegisteredWithCommit() throws Exception {
      DummyTransaction dt = startTx();
      tm().commit();
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(0, dt.getEnlistedSynchronization().size());
      assertEquals("v", cache.get("k"));
   }

   public void testSyncRegisteredWithRollback() throws Exception {
      DummyTransaction dt = startTx();
      tm().rollback();
      assertEquals(null, cache.get("k"));
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(0, dt.getEnlistedSynchronization().size());
   }

   private DummyTransaction startTx() throws NotSupportedException, SystemException {
      tm().begin();
      cache.put("k","v");
      DummyTransaction dt = (DummyTransaction) tm().getTransaction();
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(1, dt.getEnlistedSynchronization().size());
      cache.put("k2","v2");
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(1, dt.getEnlistedSynchronization().size());
      return dt;
   }
}
