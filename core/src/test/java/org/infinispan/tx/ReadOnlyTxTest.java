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
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.ReadOnlyTxTest")
@CleanupAfterMethod
public class ReadOnlyTxTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.LOCAL, true);
      return new DefaultCacheManager(configuration);
   }

   public void testSimpleReadOnlTx() throws Exception {
      tm().begin();
      assert cache.get("k") == null;
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && localTransaction.isReadOnly();
   }

   public void testNotROWhenHasWrites() throws Exception {
      tm().begin();
      cache.put("k", "v");
      assert !TestingUtil.extractLockManager(cache).isLocked("k");
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && !localTransaction.isReadOnly();
   }

   public void testNotROWhenHasOnlyLocks() throws Exception {
      cache.put("k", "v");
      tm().begin();
      cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get("k");
      assert !TestingUtil.extractLockManager(cache).isLocked("k");
      Transaction transaction = tm().suspend();
      LocalXaTransaction localTransaction = (LocalXaTransaction) txTable().getLocalTransaction(transaction);
      assert localTransaction != null && !localTransaction.isReadOnly();
   }


   private TransactionTable txTable() {
      return TestingUtil.getTransactionTable(cache);
   }
}
