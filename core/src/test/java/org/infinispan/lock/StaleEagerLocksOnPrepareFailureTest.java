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
package org.infinispan.lock;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultHashSeed;
import org.infinispan.interceptors.DistributionInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.testng.Assert.assertNull;

@Test(testName = "lock.StaleEagerLocksOnPrepareFailureTest", groups = "functional")
@CleanupAfterMethod
public class StaleEagerLocksOnPrepareFailureTest extends MultipleCacheManagersTest {

   Cache<MagicKey, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      // TODO Migrate to new pessimistic locking configuration
      cfg.setUseEagerLocking(true);
      cfg.setEagerLockSingleNode(true);
      cfg.setLockAcquisitionTimeout(100);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
      waitForClusterToForm();
   }

   public void testNoModsCommit() throws Exception {
      doTest(false);
   }

   public void testModsCommit() throws Exception {
      doTest(true);
   }

   private void doTest(boolean mods) throws Exception {
      // force the prepare command to fail on c2
      FailInterceptor interceptor = new FailInterceptor();
      interceptor.failFor(PrepareCommand.class);
      InterceptorChain ic = TestingUtil.extractComponent(c2, InterceptorChain.class);
      ic.addInterceptorBefore(interceptor, DistributionInterceptor.class);

      MagicKey k1 = new MagicKey(c1, "k1");
      MagicKey k2 = new MagicKey(c2, "k2");

      tm(c1).begin();
      if (mods) {
         c1.put(k1, "v1");
         c1.put(k2, "v2");

         assertKeyLockedCorrectly(k1);
         assertKeyLockedCorrectly(k2);
      } else {
         c1.getAdvancedCache().lock(k1);
         c1.getAdvancedCache().lock(k2);

         assertNull(c1.get(k1));
         assertNull(c1.get(k2));

         assertKeyLockedCorrectly(k1);
         assertKeyLockedCorrectly(k2);
      }

      try {
         tm(c1).commit();
         assert false : "Commit should have failed";
      } catch (Exception e) {
         // expected
      }

      assertNotLocked(c1, k1);
      assertNotLocked(c2, k1);
      assertNotLocked(c1, k2);
      assertNotLocked(c2, k2);
   }
}

