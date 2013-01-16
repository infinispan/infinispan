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

import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests verifying Querying on REPL cache configured with InfinispanIndexManager and infinispan directory provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithInfinispanDirectoryTest")
public class ClusteredCacheWithInfinispanDirectoryTest extends ClusteredCacheTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("default.directory_provider", getDirectoryProvider())
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_36")
            .addProperty("default.exclusive_index_use", "false");
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);

      ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheManagers.get(0).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
      cacheManagers.get(0).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());

      cacheManagers.get(1).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
      cacheManagers.get(1).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());
   }

   public String getDirectoryProvider() {
      return "infinispan";
   }
}
