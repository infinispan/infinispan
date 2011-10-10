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
package org.infinispan.notifications;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Used to verify which nodes are going to receive events in case it's configured
 * as DIST: all key owners and the node which is performing the operation will receive
 * a notification.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
@Test(groups = "functional", testName = "distribution.DistListenerTest")
public class DistListenerTest extends MultipleCacheManagersTest {

   private TestListener listener;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(Configuration.CacheMode.DIST_SYNC, 3);
      waitForClusterToForm();
   }
   
   public void testRemoteGet() {
      final String key1 = this.getClass().getName() + "K1";

      List<Address> owners = cache(0).getAdvancedCache().getDistributionManager().locate(key1);

      assert owners.size() == 2: "Key should have 2 owners";

      Cache owner1 = getCacheForAddress(owners.get(0));
      Cache owner2 = getCacheForAddress(owners.get(1));
      assert owner1 != owner2;
      Cache nonOwner = null;
      for (int i=0; i<3; i++) {
         if (cache(i) != owner1 && cache(i) != owner2) {
            nonOwner = cache(i);
            break;
         }
      }
      assert nonOwner != null;
      
      listener = new TestListener();
      
      // test owner puts and listens:
      assertCreated(false);
      assertModified(false);
      owner1.addListener(listener);
      owner1.put(key1, "hello");
      assertModified(true);
      assertCreated(true);
      assertCreated(false);
      assertModified(false);
      owner1.put(key1, "hello");
      assertModified(true);
      assertCreated(false);
      
      // now de-register:
      owner1.removeListener(listener);
      owner1.put(key1, "hello");
      assertModified(false);
      assertCreated(false);
      
      // put on non-owner and listens on owner:
      owner1.addListener(listener);
      nonOwner.put(key1, "hello");
      assertModified(true);
      owner1.removeListener(listener);
      assertModified(false);
      
      //listen on non-owner:
      nonOwner.addListener(listener);
      nonOwner.put(key1, "hello");
      assertModified(true);
      
      //listen on non-owner non-putting:
      owner1.put(key1, "hello");
      assertModified(false);
   }
   
   private void assertCreated(boolean b) {
      assert listener.created == b;
      listener.created = false;
   }
   
   private void assertModified(boolean b) {
      assert listener.modified == b;
      listener.modified = false;
   }

   @SuppressWarnings("unchecked")
   private Cache<MagicKey, String> getCacheForAddress(Address a) {
      for (Cache<?, ?> c: caches())
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(a)) return (Cache<MagicKey, String>) c;
      return null;
   }
   
   @Listener
   public class TestListener {
      
      boolean created = false;
      boolean modified = false;
      
      @CacheEntryCreated
      public void create(CacheEntryEvent e) {
         created = true;
      }
      
      @CacheEntryModified
      public void modify(CacheEntryEvent e) {
         modified = true;
      }
   }

}
