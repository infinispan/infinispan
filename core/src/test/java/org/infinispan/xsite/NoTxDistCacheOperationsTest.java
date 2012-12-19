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

package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.NoTxDistCacheOperationsTest")
public class NoTxDistCacheOperationsTest extends BaseCacheOperationsTest {

   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   public void testDataGetsReplicated() {
      cache("LON", 0).put("k_lon", "v_lon");
      assertNull(cache("NYC", 0).get("k_lon"));
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 0).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 1).get("k_lon"), "v_lon");

      cache("NYC",1).put("k_nyc", "v_nyc");
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("LON", "nycBackup", 0).get("k_nyc"), "v_nyc");
      assertEquals(cache("LON", "nycBackup", 1).get("k_nyc"), "v_nyc");
      assertNull(cache("LON", 0).get("k_nyc"));

      cache("LON", 1).remove("k_lon");
      assertNull(cache("LON", 1).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 0).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 1).get("k_lon"));
   }
}
