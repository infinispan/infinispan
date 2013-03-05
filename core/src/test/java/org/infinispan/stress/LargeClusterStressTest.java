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

package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@Test(groups = "stress", testName = "statetransfer.LargeClusterStressTest")
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 64;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeCluster() {
      ConfigurationBuilder distConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      ConfigurationBuilder replConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      for (int i = 0; i < NUM_NODES; i++) {
         defineConfigurationOnAllManagers("dist", distConfig);
         defineConfigurationOnAllManagers("repl", replConfig);
         EmbeddedCacheManager cm = addClusterEnabledCacheManager();
         Cache<Object,Object> replCache = cm.getCache("repl");
         Cache<Object, Object> distCache = cm.getCache("dist");

         replCache.put(cm.getAddress(), "bla");

         waitForClusterToForm("repl");
         waitForClusterToForm("dist");
      }
   }
}
