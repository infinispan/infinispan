/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.distributed;

import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying that Mass Indexer works properly on Topology Aware nodes.
 */
@Test(groups = "functional", testName = "query.distributed.ClusteredQueryMassIndexingTest")
public class TopologyAwareDistMassIndexingTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(NUM_NODES, CacheMode.DIST_SYNC, false, true, false);

      for(Object cache : caches) {
         Cache cacheObj = (Cache) cache;
         EmbeddedCacheManager cm1 = cacheObj.getCacheManager();
         ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
         cm1.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
         cm1.defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());

         cacheManagers.add(cm1);
      }

      waitForClusterToForm(neededCacheNames);
   }
}