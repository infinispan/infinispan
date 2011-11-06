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
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleLocksOnPrepareFailureTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksOnPrepareFailureTest extends MultipleCacheManagersTest {

   public static final int NUM_CACHES = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      cfg.setNumOwners(NUM_CACHES);
      cfg.setLockAcquisitionTimeout(100);
      for (int i = 0; i < NUM_CACHES; i++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(cfg);
         registerCacheManager(cm);
      }
      waitForClusterToForm();
   }

   public void testModsCommit() throws Exception {
      doTest(true);
   }

   private void doTest(boolean mods) throws Exception {
      Cache<Object, Object> c1 = cache(0);
      Cache<Object, Object> c2 = cache(NUM_CACHES /2);

      // force the prepare command to fail on c2
      FailInterceptor interceptor = new FailInterceptor();
      interceptor.failFor(PrepareCommand.class);
      InterceptorChain ic = TestingUtil.extractComponent(c2, InterceptorChain.class);
      ic.addInterceptorBefore(interceptor, DistributionInterceptor.class);

      MagicKey k1 = new MagicKey(c1, "k1");

      tm(c1).begin();
      c1.put(k1, "v1");

      try {
         tm(c1).commit();
         assert false : "Commit should have failed";
      } catch (Exception e) {
         // expected
      }

      for (int i = 0; i < NUM_CACHES; i++) {
        assertNotLocked(cache(i), k1);
      }
   }
}

