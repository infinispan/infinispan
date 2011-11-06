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
import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "functional", testName = "distribution.RemoteGetTest")
public class RemoteGetTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(Configuration.CacheMode.DIST_SYNC, 3);
      // make sure all caches are started...
      cache(0);
      cache(1);
      cache(2);
      waitForClusterToForm();
   }

   @SuppressWarnings("unchecked")
   private Cache<MagicKey, String> getCacheForAddress(Address a) {
      for (Cache<?, ?> c: caches())
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(a)) return (Cache<MagicKey, String>) c;
      return null;
   }

   @SuppressWarnings("unchecked")
   private Cache<MagicKey, String> getNonOwner(List<Address> a) {
      for (Cache<?, ?> c: caches())
         if (!a.contains(c.getAdvancedCache().getRpcManager().getAddress())) return (Cache<MagicKey, String>) c;
      return null;
   }

   public void testRemoteGet() {
      MagicKey k = new MagicKey(cache(0)); // this should now map to cache0 and cache1

      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(k);

      assert owners.size() == 2: "Key should have 2 owners";

      Cache<MagicKey, String> owner1 = getCacheForAddress(owners.get(0));
      Cache<MagicKey, String> owner2 = getCacheForAddress(owners.get(1));
      Cache<MagicKey, String> nonOwner = getNonOwner(owners);

      owner1.put(k, "value");
      assert "value".equals(nonOwner.get(k));
   }

   public void testGetOfNonexistentKey() {
      Object v = cache(0).get("__ doesn't exist ___");
      assert v == null : "Should get a null response";
   }

   public void testGetOfNonexistentKeyOnOwner() {
      MagicKey mk = new MagicKey(cache(0), "does not exist");
      Object v = cache(0).get(mk);
      assert v == null : "Should get a null response";
   }
}
