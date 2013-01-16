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

import junit.framework.Assert;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests verifying that the Mass Indexing for programmatic cache configuration works as well.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.DistProgrammaticMassIndex")
public class DistProgrammaticMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("hibernate.search.default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty("hibernate.search.default.exclusive_index_use", "false")
            .addProperty("lucene_version", "LUCENE_36");
      List<Cache<String, Car>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      for(int i = 0; i < NUM_NODES; i++) {
         ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
         cacheManagers.get(i).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
         cacheManagers.get(i).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());
      }

      waitForClusterToForm(neededCacheNames);

      for(Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   protected void verifyFindsCar(Cache cache, int count, String carMake) {
      QueryParser queryParser = createQueryParser("make");

      try {
         Query luceneQuery = queryParser.parse(carMake);
         CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, Car.class);

         Assert.assertEquals(count, cacheQuery.getResultSize());

      } catch(ParseException ex) {
         ex.printStackTrace();
         Assert.fail("Failed due to: " + ex.getMessage());
      }
   }

}