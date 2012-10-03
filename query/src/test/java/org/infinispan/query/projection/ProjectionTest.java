/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.projection;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Test(groups = "functional", testName = "query.projection.ProjectionTest")
public class ProjectionTest extends SingleCacheManagerTest {

   private SearchManager searchManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cfg);
      Cache<Object, Object> cache = cacheManager.getCache();
      searchManager = Search.getSearchManager(cache);
      return cacheManager;
   }

   @Test
   public void testQueryProjectionWithSingleField() throws Exception {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery cacheQuery = createProjectionQuery("bar");
      assertQueryReturns(cacheQuery, new Object[] { "bar1" });
   }

   @Test
   public void testQueryProjectionWithMultipleFields() throws Exception {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery cacheQuery = createProjectionQuery("bar", "baz");
      assertQueryReturns(cacheQuery, new Object[] { "bar1", "baz1" });
   }

   @Test
   public void testKeyProjectionConstant() throws Exception {
      cache.put("1", new Foo("bar1", "baz1"));
      CacheQuery cacheQuery = createProjectionQuery(ProjectionConstants.KEY);
      assertQueryReturns(cacheQuery, new Object[] { "1" });
   }

   @Test
   public void testValueProjectionConstant() throws Exception {
      Foo foo = new Foo("bar1", "baz1");
      cache.put("1", foo);
      CacheQuery cacheQuery = createProjectionQuery(ProjectionConstants.VALUE);
      assertQueryReturns(cacheQuery, new Object[] { foo });
   }

   @Test
   public void testMixedProjections() throws Exception {
      Foo foo = new Foo("bar1", "baz4");
      cache.put("1", foo);
      CacheQuery cacheQuery = createProjectionQuery(
            ProjectionConstants.KEY,
            ProjectionConstants.VALUE,
            ProjectionConstants.VALUE,
            org.hibernate.search.ProjectionConstants.OBJECT_CLASS,
            "baz",
            "bar"
            );
      assertQueryReturns(cacheQuery, new Object[] { "1", foo, foo, foo.getClass(), foo.baz, foo.bar });
   }

   private CacheQuery createProjectionQuery(String... projection) {
      QueryBuilder queryBuilder = searchManager.buildQueryBuilderForClass(Foo.class).get();
      Query query = queryBuilder.keyword().onField("bar").matching("bar1").createQuery();
      CacheQuery cacheQuery = searchManager.getQuery(query);
      cacheQuery.projection(projection);
      return cacheQuery;
   }

   private void assertQueryReturns(CacheQuery cacheQuery, Object[] expected) {
      assertQueryListContains(cacheQuery.list(), expected);
      assertQueryIteratorContains(cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER)), expected);
      assertQueryIteratorContains(cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY)), expected);
   }

   private void assertQueryListContains(List list, Object[] expected) {
      assert list.size() == 1;
      Object[] array = (Object[]) list.get(0);
      Assert.assertArrayEquals(expected, array);
   }

   private void assertQueryIteratorContains(ResultIterator iterator, Object[] expected) {
      assert iterator.hasNext();
      Object[] array = (Object[]) iterator.next();
      assert Arrays.equals(array, expected);
      assert !iterator.hasNext();
   }

   @Indexed(index = "FooIndex")
   public static class Foo {
      private String bar;
      private String baz;

      public Foo(String bar, String baz) {
         this.bar = bar;
         this.baz = baz;
      }

      @Field(name = "bar", store = Store.YES)
      public String getBar() {
         return bar;
      }

      @Field(name = "baz", store = Store.YES)
      public String getBaz() {
         return baz;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o)
            return true;
         if (o == null || getClass() != o.getClass())
            return false;

         Foo foo = (Foo) o;

         if (bar != null ? !bar.equals(foo.bar) : foo.bar != null)
            return false;
         if (baz != null ? !baz.equals(foo.baz) : foo.baz != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return bar.hashCode();
      }
   }

}
