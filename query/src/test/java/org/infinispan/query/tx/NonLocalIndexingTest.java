/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.query.tx;

import static org.infinispan.test.TestingUtil.withTx;

import java.util.concurrent.Callable;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test to make sure indexLocalOnly=false behaves the same with or without
 * transactions enabled.
 * See also ISPN-2467 and subclasses.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.tx.NonLocalIndexingTest")
public class NonLocalIndexingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      builder.indexing().enable().indexLocalOnly(false)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(2, builder);
   }

   //Extension point to override configuration
   protected boolean transactionsEnabled() {
      return false;
   }

   public void testQueryAfterAddingNewNode() throws Exception {
      store(new Person("Astronaut","is asking his timezone", 32), cache(0));
      assertFind("timezone", 1);
      assertFind("asking", 1);
      assertFind("cat", 0);
      store(new Person("Webdeveloper","is confused by the timezone concept", 32), cache(1));
      assertFind("cat", 0);
      assertFind("timezone", 2);
      //We're using the name as a key so this is an update in practice of the Astronaut record:
      store(new Person("Astronaut","thinks about his cat", 32), cache(1));
      assertFind("cat", 1);
      assertFind("timezone", 1);
   }

   private void assertFind(String keyword, int expectedCount) {
      assertFind(cache(0), keyword, expectedCount);
      assertFind(cache(1), keyword, expectedCount);
   }

   private static void assertFind(Cache cache, String keyword, int expectedCount) {
      SearchManager queryFactory = Search.getSearchManager(cache);
      Query luceneQuery = new TermQuery(new Term("blurb", keyword));
      CacheQuery cacheQuery = queryFactory.getQuery(luceneQuery, Person.class);
      int resultSize = cacheQuery.getResultSize();
      Assert.assertEquals(resultSize, expectedCount);
   }

   private void store(final Person person, final Cache<Object, Object> cache) throws Exception {
      Callable<Void> callable = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            cache.put(person.getName(), person);
            return null;
         }
      };
      if (transactionsEnabled()) {
         withTx(cache.getAdvancedCache().getTransactionManager(), callable);
      }
      else {
         callable.call();
      }
   }

}
