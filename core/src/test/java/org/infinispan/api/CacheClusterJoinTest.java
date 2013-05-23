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
package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups = "functional", testName = "api.CacheClusterJoinTest")
public class CacheClusterJoinTest extends MultipleCacheManagersTest {

   private EmbeddedCacheManager cm1, cm2;
   private ConfigurationBuilder cfg;

   public CacheClusterJoinTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {
      cm1 = addClusterEnabledCacheManager();
      cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      cm1.defineConfiguration("cache", cfg.build());
   }

   public void testGetMembers() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      List memb1 = cm1.getMembers();
      assert 1 == memb1.size() : "Expected 1 member; was " + memb1;

      Object coord = memb1.get(0);

      cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("cache", cfg.build());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      TestingUtil.blockUntilViewsReceived(50000, true, cm1, cm2);
      memb1 = cm1.getMembers();
      List memb2 = cm2.getMembers();
      assert 2 == memb1.size();
      assert memb1.equals(memb2);

      TestingUtil.killCacheManagers(cm1);
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      memb2 = cm2.getMembers();
      assert 1 == memb2.size();
      assert !coord.equals(memb2.get(0));
   }

   public void testIsCoordinator() throws Exception {
      cm1.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator() : "Should be coordinator!";

      cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("cache", cfg.build());
      cm2.getCache("cache"); // this will make sure any lazy components are started.
      assert cm1.isCoordinator();
      assert !cm2.isCoordinator();
      TestingUtil.killCacheManagers(cm1);
      // wait till cache2 gets the view change notification
      TestingUtil.blockUntilViewsReceived(50000, false, cm2);
      assert cm2.isCoordinator();
   }
}
