/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(groups = "functional", testName = "api.SimpleConditionalOperationTest")
public class SimpleConditionalOperationTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getConfig();
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testReplaceFromMainOwner() throws Throwable {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      tm(0).begin();
      cache(0).put("kkk", "vvv");
      cache(0).replace(k, "v1", "v2");
      tm(0).commit();

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      log.trace("here is the interesting replace.");
      cache(0).replace(k, "0", "1");
      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "1");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "1");
   }

   public void testRemoveFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).remove(k, "1");

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      cache(0).remove(k, "0");
      assertNull(advancedCache(0).getDataContainer().get(k));
      assertNull(advancedCache(1).getDataContainer().get(k));
   }

   public void testPutIfAbsentFromMainOwner() {
      Object k = getKeyForCache(0);
      cache(0).put(k, "0");
      cache(0).putIfAbsent(k, "1");

      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "0");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "0");

      cache(0).remove(k);

      cache(0).putIfAbsent(k, "1");
      assertEquals(advancedCache(0).getDataContainer().get(k).getValue(), "1");
      assertEquals(advancedCache(1).getDataContainer().get(k).getValue(), "1");
   }
}
