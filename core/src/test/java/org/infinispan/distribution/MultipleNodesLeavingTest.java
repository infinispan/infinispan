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

import org.infinispan.config.Configuration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "distribution.MultipleNodesLeavingTest")
public class MultipleNodesLeavingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC), 4);
      waitForClusterToForm();
   }

   public void testMultipleLeaves() throws Exception {

      //kill 3 caches at once
      fork(new Runnable() {
         @Override
         public void run() {
            manager(3).stop();
         }
      }, false);

      fork(new Runnable() {
         @Override
         public void run() {
            manager(2).stop();
         }
      }, false);

      fork(new Runnable() {
         @Override
         public void run() {
            manager(1).stop();
         }
      }, false);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            List<Address> members = advancedCache(0).getRpcManager().getTransport().getMembers();
            System.out.println("members = " + members);
            return members.size() == 1;
         }
      });

      System.out.println("MultipleNodesLeavingTest.testMultipleLeaves");

      TestingUtil.blockUntilViewsReceived(60000, false, cache(0));
      TestingUtil.waitForRehashToComplete(cache(0));
      Set<Address> caches = advancedCache(0).getDistributionManager().getConsistentHash().getCaches();
      System.out.println("caches = " + caches);
      int size = caches.size();
      assert size == 1;
   }
}
