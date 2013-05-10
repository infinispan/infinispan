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

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.locking.LocalOptimisticTxTest")
public class LocalOptimisticTxTest extends AbstractLocalTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final Configuration config = getDefaultStandaloneConfig(true);
      config.fluent().transaction().lockingMode(LockingMode.OPTIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Override
   protected void assertLockingOnRollback() {
      assertFalse(lockManager().isLocked("k"));
      rollback();
      assertFalse(lockManager().isLocked("k"));
   }

   protected void assertLocking() {
      assertFalse(lockManager().isLocked("k"));
      prepare();
      assertTrue(lockManager().isLocked("k"));
      commit();
      assertFalse(lockManager().isLocked("k"));
   }
}
