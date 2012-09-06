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
      this.INIT_CLUSTER_SIZE = 2;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   EmbeddedCacheManager joinerManager;
   Cache<Object, String> joiner;

   void performRehashEvent() {
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
      joiner = joinerManager.getCache(cacheName);
   }

   int waitForJoinCompletion() {
      // need to block until this join has completed!
      TestingUtil.blockUntilViewsReceived(SECONDS.toMillis(10), c1, c2, joiner);
      waitForClusterToForm(cacheName);

      caches.add(joiner);
      return caches.size() - 1;
   }

   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey("k1", c1), new MagicKey("k2", c2)
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      log.infof("Initialized with keys %s", keys);
      return keys;
   }

   public void testInvalidationBehaviorOnRehash() {
      // start with 2 caches...
      List<MagicKey> keys = init();
      // add 1
      performRehashEvent();
      int joinerPos = waitForJoinCompletion();

      for (MagicKey key : keys) {
         assertOwnershipAndNonOwnership(key, l1OnRehash);
      }
   }
}
