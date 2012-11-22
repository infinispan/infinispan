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
package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ensures entries are moved to L1 if they are removed due to a rehash
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1OnRehashTest")
public class L1OnRehashTest extends BaseDistFunctionalTest {
   public L1OnRehashTest() {
      this.tx = false;
      this.sync = true;
      this.l1CacheEnabled = true;
      this.performRehashing = true;
      this.l1OnRehash = true;
      this.INIT_CLUSTER_SIZE = 3;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   EmbeddedCacheManager joinerManager1, joinerManager2;
   Cache<Object, String> joiner1, joiner2;

   void performRehashEvent() {
      joinerManager1 = addClusterEnabledCacheManager();
      joinerManager1.defineConfiguration(cacheName, configuration.build());
      joiner1 = joinerManager1.getCache(cacheName);
      joinerManager2 = addClusterEnabledCacheManager();
      joinerManager2.defineConfiguration(cacheName, configuration.build());
      joiner2 = joinerManager2.getCache(cacheName);

      // need to block until this join has completed!
      TestingUtil.blockUntilViewsReceived(SECONDS.toMillis(10), c1, c2, c3, joiner1, joiner2);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, joiner1, joiner2);

      caches.add(joiner1);
      caches.add(joiner2);
   }

   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey("k1", c1), new MagicKey("k2", c3), new MagicKey("k3", c2)
      ));

      for (int i = 0; i < keys.size(); i++) {
         Cache<Object, String> c = cache(i, cacheName);
         c.put(keys.get(i), "v" + (i + 1));
      }

      for (int i = 0; i < keys.size(); i++) {
         Object key = keys.get(i);
         assertOwnershipAndNonOwnership(key, l1CacheEnabled);
         // checking the values will bring the keys to L1 on all the caches
         assertOnAllCaches(key, "v" + (i + 1));
      }

      log.infof("Initialized with keys %s", keys);
      return keys;
   }

   public void testInvalidationBehaviorOnRehash() {
      // start with 2 caches...
      List<MagicKey> keys = init();
      // add 2 nodes
      performRehashEvent();

      for (MagicKey key : keys) {
         // even if L1.onRehash is disabled, it's ok for some L1 entries to be still valid after the rehash
         assertOwnershipAndNonOwnership(key, true);
      }

      // invalidate on everyone but the owners
      for (int i = 0; i < keys.size(); i++) {
         Object key = keys.get(i);
         Cache<Object, String> owner = getLockOwner(key, cacheName);
         owner.put(keys.get(i), "nv" + (i + 1));
      }

      for (int i = 0; i < keys.size(); i++) {
         Object key = keys.get(i);
         // TODO Change the 2nd parameter to false when https://issues.jboss.org/browse/ISPN-2475 is fixed
         assertOwnershipAndNonOwnership(key, l1OnRehash);
         // TODO Remove this check when https://issues.jboss.org/browse/ISPN-2475 is fixed
         if (!l1OnRehash) {
            assertOnAllCaches(key, "nv" + (i + 1));
         }
      }
   }
}
