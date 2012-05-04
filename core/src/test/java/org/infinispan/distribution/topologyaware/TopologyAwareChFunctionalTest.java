/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.topologyaware;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.DistSyncFuncTest;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "distribution.TopologyAwareChFunctionalTest")
public class TopologyAwareChFunctionalTest extends DistSyncFuncTest {

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      int index = cacheManagers.size();
      String rack;
      String machine;
      switch (index) {
         case 0 : {
            rack = "r0";
            machine = "m0";
            break;
         }
         case 1 : {
            rack = "r0";
            machine = "m1";
            break;
         }
         case 2 : {
            rack = "r1";
            machine = "m0";
            break;
         }
         case 3 : {
            rack = "r2";
            machine = "m0";
            break;
         }
         default : {
            throw new RuntimeException("Bad!");
         }
      }
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      gc.setRackId(rack);
      gc.setMachineId(machine);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gc, new Configuration());
      cacheManagers.add(cm);
      return cm;
   }

   public void testHashesInitiated() {
      TopologyAwareConsistentHash hash = (TopologyAwareConsistentHash) advancedCache(0, cacheName).getDistributionManager().getConsistentHash();
      containsAllHashes(hash);
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(1, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(2, cacheName).getDistributionManager().getConsistentHash());
      containsAllHashes((TopologyAwareConsistentHash) advancedCache(3, cacheName).getDistributionManager().getConsistentHash());
   }

   private void containsAllHashes(TopologyAwareConsistentHash ch) {
      assert ch.getCaches().contains(address(0));
      assert ch.getCaches().contains(address(1));
      assert ch.getCaches().contains(address(2));
      assert ch.getCaches().contains(address(3));
   }
}
