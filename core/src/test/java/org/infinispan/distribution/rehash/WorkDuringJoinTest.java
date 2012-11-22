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
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests performing some work on the joiner during a JOIN
 *
 * @author Manik Surtani
 * @since 4.0
 */
// TODO This test makes no sense anymore, now that a joiner blocks until the join completes, before returning from cache.start().  This test needs to be re-thought and redesigned to test the eventual consistency (UnsureResponse) of a remote GET properly.
@Test(groups = "functional", testName = "distribution.rehash.WorkDuringJoinTest", enabled = false)
public class WorkDuringJoinTest extends BaseDistFunctionalTest {

   EmbeddedCacheManager joinerManager;
   Cache<Object, String> joiner;

   public WorkDuringJoinTest() {
      INIT_CLUSTER_SIZE = 2;
   }

   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey("k1", c1), new MagicKey("k2", c2),
            new MagicKey("k3", c1), new MagicKey("k4", c2)
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      log.infof("Initialized with keys %s", keys);
      return keys;
   }

   Address startNewMember() {
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration.build());
      joiner = joinerManager.getCache(cacheName);
      return manager(joiner).getAddress();
   }

   public void testJoinAndGet() {
      List<MagicKey> keys = init();
      ConsistentHash chOld = getConsistentHash(c1);
      Address joinerAddress = startNewMember();
      List<Address> newMembers = new ArrayList<Address>(chOld.getMembers());
      newMembers.add(joinerAddress);
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      ConsistentHash chNew = chf.rebalance(chf.updateMembers((DefaultConsistentHash) chOld, newMembers));
      // which key should me mapped to the joiner?
      MagicKey keyToTest = null;
      for (MagicKey k: keys) {
         if (chNew.isKeyLocalToNode(joinerAddress, k)) {
            keyToTest = k;
            break;
         }
      }

      if (keyToTest == null) throw new NullPointerException("Couldn't find a key mapped to J!");
      assert joiner.get(keyToTest) != null;
   }
}
