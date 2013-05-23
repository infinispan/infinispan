/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.expiry;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Test(groups = "functional", testName = "expiry.ReplicatedExpiryTest")
public class ReplicatedExpiryTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      createClusteredCaches(2, "cache", builder);
   }

   public void testLifespanExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long lifespan = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof MortalCacheEntry;
      assert ice.getLifespan() == lifespan;
      assert ice.getMaxIdle() == -1;
   }

   public void testIdleExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long idle = 3000;
      c1.put("k", "v", -1, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");

      assert ice instanceof TransientCacheEntry;
      assert ice.getMaxIdle() == idle;
      assert ice.getLifespan() == -1;
   }

   public void testBothExpiryReplicates() {
      Cache c1 = cache(0,"cache");
      Cache c2 = cache(1,"cache");
      long lifespan = 10000;
      long idle = 3000;
      c1.put("k", "v", lifespan, MILLISECONDS, idle, MILLISECONDS);
      InternalCacheEntry ice = c2.getAdvancedCache().getDataContainer().get("k");
      assert ice instanceof TransientMortalCacheEntry;
      assert ice.getLifespan() == lifespan : "Expected " + lifespan + " but was " + ice.getLifespan();
      assert ice.getMaxIdle() == idle : "Expected " + idle + " but was " + ice.getMaxIdle();
   }
}
