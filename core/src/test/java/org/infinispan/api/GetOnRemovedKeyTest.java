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

package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test (groups = "functional", testName = "api.GetOnRemovedKeyTest")
public class GetOnRemovedKeyTest extends MultipleCacheManagersTest {

   protected CacheMode mode = CacheMode.REPL_SYNC;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(mode, true), 2);
      waitForClusterToForm();
   }

   public void testRemoveSeenCorrectly1() throws Throwable {
      Object k = getKey();
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).remove(k);
      assertNull(cache(0).get(k));
      tm(0).commit();
      assertNull(cache(0).get(k));
   }

   public void testRemoveSeenCorrectly2() throws Throwable {
      Object k = getKey();
      cache(0).put(k, "v");
      tm(0).begin();
      cache(0).remove(k);
      assertNull(cache(0).get(k));
      tm(0).rollback();
      assertEquals("v", cache(0).get(k));
   }

   protected Object getKey() {
      return "k";
   }
}
