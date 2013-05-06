/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.api.batch;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = "functional", testName = "api.batch.LockInBatchTest")
public class LockInBatchTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dccc.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC);
      dccc.invocationBatching().enable(true);
      return TestCacheManagerFactory.createCacheManager(dccc);
   }

   public void testLockWithBatchingRollback() {
      cache.startBatch();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      cache().endBatch(false);
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithBatchingCommit() {
      cache.startBatch();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      cache().endBatch(true);
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithTmRollback() throws Throwable {
      tm().begin();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      tm().rollback();
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithTmCommit() throws Throwable {
      tm().begin();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      tm().commit();
      assertFalse(lockManager().isLocked("k"));
   }
}
