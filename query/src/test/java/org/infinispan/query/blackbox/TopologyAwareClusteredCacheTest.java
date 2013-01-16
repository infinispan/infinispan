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

package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Testing the query functionality on clustered caches started on TopologyAware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.TopologyAwareClusteredCacheTest")
public class TopologyAwareClusteredCacheTest extends ClusteredCacheTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      List caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(2, getCacheMode(), transactionEnabled(),
                                                                         isIndexLocalOnly(), isRamDirectory());

      for(Object cache : caches) {
         cacheManagers.add(((Cache) cache).getCacheManager());
      }

      cache1 = (Cache<String, Person>) caches.get(0);
      cache2 = (Cache<String, Person>) caches.get(1);

      waitForClusterToForm();
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   public boolean isIndexLocalOnly() {
      return false;
   }

   public boolean isRamDirectory() {
      return true;
   }

   public boolean transactionEnabled() {
      return false;
   }
}
