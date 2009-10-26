/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.distribution;


import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Ispn234Test.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "unit", testName = "distribution.Ispn234Test")
public class SingleOwnerTest extends BaseDistFunctionalTest {
   
   @Override
   protected void createCacheManagers() throws Throwable {
      cacheName = "dist";
      configuration = getDefaultClusteredConfig(sync ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC, tx);
      if (!testRetVals) {
         configuration.setUnsafeUnreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      configuration.setSyncReplTimeout(60, TimeUnit.SECONDS);
      configuration.setNumOwners(1);
      configuration.setLockAcquisitionTimeout(45, TimeUnit.SECONDS);
      caches = createClusteredCaches(2, cacheName, configuration);

      c1 = caches.get(0);
      c2 = caches.get(1);

      cacheAddresses = new ArrayList<Address>(2);
      for (Cache cache : caches) cacheAddresses.add(cache.getCacheManager().getAddress());

      RehashWaiter.waitForInitRehashToComplete(c1, c2);
   }

   public void testPutOnKeyOwner() {
      Cache[] caches = getOwners("mykey", 1);
      assert caches.length == 1;
      Cache ownerCache = caches[0];
      ownerCache.put("mykey", new Object());
   }

}
