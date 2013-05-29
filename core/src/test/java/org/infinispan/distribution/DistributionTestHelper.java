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
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * Test helper class
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class DistributionTestHelper {
   public static String safeType(Object o) {
      if (o == null) return "null";
      return o.getClass().getSimpleName();
   }

   public static void assertIsInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice != null : "Entry for key [" + key + "] should be in L1 on cache at [" + addressOf(cache) + "]!";
      assert !(ice instanceof ImmortalCacheEntry) : "Entry for key [" + key + "] should have a lifespan on cache at [" + addressOf(cache) + "]!";
   }

   public static void assertIsNotInL1(Cache<?, ?> cache, Object key) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      assert ice == null : "Entry for key [" + key + "] should not be in data container at all on cache at [" + addressOf(cache) + "]!";
   }

   public static void assertIsInContainerImmortal(Cache<?, ?> cache, Object key) {
      Log log = LogFactory.getLog(BaseDistFunctionalTest.class);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      if (ice == null) {
         String msg = "Entry for key [" + key + "] should be in data container on cache at [" + addressOf(cache) + "]!";
         log.fatal(msg);
         assert false : msg;
      }

      if (!(ice instanceof ImmortalCacheEntry)) {
         String msg = "Entry for key [" + key + "] on cache at [" + addressOf(cache) + "] should be immortal but was [" + ice + "]!";
         log.fatal(msg);
         assert false : msg;
      }
   }

   public static void assertIsInL1OrNull(Cache<?, ?> cache, Object key) {
      Log log = LogFactory.getLog(BaseDistFunctionalTest.class);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.get(key);
      if (ice instanceof ImmortalCacheEntry) {
         String msg = "Entry for key [" + key + "] on cache at [" + addressOf(cache) + "] should be mortal or null but was [" + ice + "]!";
         log.fatal(msg);
         assert false : msg;
      }
   }


   public static boolean isOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getDistributionManager();
      List<Address> ownerAddresses = dm.locate(key);
      return ownerAddresses.contains(addressOf(c));
   }

   public static boolean isFirstOwner(Cache<?, ?> c, Object key) {
      DistributionManager dm = c.getAdvancedCache().getDistributionManager();
      Address primaryOwnerAddress = dm.getPrimaryLocation(key);
      return addressOf(c).equals(primaryOwnerAddress);
   }

   public static boolean hasOwners(Object key, Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      DistributionManager dm = primaryOwner.getAdvancedCache().getDistributionManager();
      List<Address> ownerAddresses = dm.locate(key);
      if (!addressOf(primaryOwner).equals(ownerAddresses.get(0)))
         return false;
      for (int i = 0; i < backupOwners.length; i++) {
         if (!ownerAddresses.contains(addressOf(backupOwners[i])))
            return false;
      }
      return true;
   }

   public static Address addressOf(Cache<?, ?> cache) {
      EmbeddedCacheManager cacheManager = cache.getCacheManager();
      return cacheManager.getAddress();

   }
}
