/*
 * JBoss, Home of Professional Open Source
 *  Copyright 2013 Red Hat Inc. and/or its affiliates and other
 *  contributors as indicated by the @author tags. All rights reserved
 *  See the copyright.txt in the distribution for a full listing of
 *  individual contributors.
 *
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation; either version 2.1 of
 *  the License, or (at your option) any later version.
 *
 *  This software is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this software; if not, write to the Free
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * https://issues.jboss.org/browse/ISPN-3141
 */
@Test(groups = "functional", testName = "api.IgnoreReturnValueForConditionalOperationsTest")
public class IgnoreReturnValueForConditionalOperationsTest extends MultipleCacheManagersTest {

   protected boolean transactional;

   @Override
   protected void createCacheManagers() throws Throwable {
      transactional = false;
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, transactional);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }


   public void testConditionalReplace() {
      Object k = init();
      assertTrue(advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).replace(k, "v0", "v1"));
      assertEquals(cache(0).get(k), "v1");
      assertEquals(cache(1).get(k), "v1");
   }

   public void testConditionalRemove() {
      Object k = init();
      assertTrue(advancedCache(0).withFlags(Flag.IGNORE_RETURN_VALUES).remove(k, "v0"));
      assertNull(cache(0).get(k));
      assertNull(cache(1).get(k));
   }

   private Object init() {
      Object k = getKeyForCache(1);
      cache(0).put(k, "v0");
      assertEquals(cache(0).get(k), "v0");
      assertEquals(cache(1).get(k), "v0");
      return k;
   }
}
