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
package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleLocksTransactionTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksTransactionTest extends MultipleCacheManagersTest {

   Cache<String, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      cfg.setLockAcquisitionTimeout(100);
      cfg.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
   }

   public void testNoModsCommit() throws Exception {
      doTest(false, true);
   }

   public void testModsRollback() throws Exception {
      doTest(true, false);
   }

   public void testNoModsRollback() throws Exception {
      doTest(false, false);
   }

   public void testModsCommit() throws Exception {
      doTest(true, true);
   }

   private void doTest(boolean mods, boolean commit) throws Exception {
      tm(c1).begin();
      c1.getAdvancedCache().lock("k");
      assert c1.get("k") == null;
      if (mods) c1.put("k", "v");
      if (commit)
         tm(c1).commit();
      else
         tm(c1).rollback();

      assertNotLocked(c1, "k");
      assertNotLocked(c2, "k");
   }
}
