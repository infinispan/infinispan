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
package org.infinispan.query.searchmanager;

import org.apache.lucene.queryParser.QueryParser;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * The test covers the timeout functionality for ClusteredCacheQuery class. At the moment it is not implemented, so throws
 * UnsupportedException.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.ClusteredCacheQueryTimeoutTest")
public class ClusteredCacheQueryTimeoutTest extends MultipleCacheManagersTest {
   private Cache cache1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class, expectedExceptionsMessageRegExp = "Clustered queries do not support timeouts yet.")
   public void testClusteredQueryCacheTimeout() throws Exception {
      SearchManager searchManager = Search.getSearchManager(cache1);

      QueryParser queryParser = createQueryParser("bar");

      org.apache.lucene.search.Query luceneQuery = queryParser.parse("fakebar");
      CacheQuery query = searchManager.getClusteredQuery(luceneQuery, Foo.class);
      query.timeout( 1, TimeUnit.NANOSECONDS );
   }

   @Indexed(index = "FooIndex")
   public class Foo {
      private String bar;

      public Foo(String bar) {
         this.bar = bar;
      }

      @Field(name = "bar")
      public String getBar() {
         return bar;
      }
   }
}