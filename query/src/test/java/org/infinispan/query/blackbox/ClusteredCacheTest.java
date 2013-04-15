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
package org.infinispan.query.blackbox;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * @author Navin Surtani
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheTest")
public class ClusteredCacheTest extends MultipleCacheManagersTest {

   Cache<String, Person> cache1, cache2;
   Person person1;
   Person person2;
   Person person3;
   Person person4;
   QueryParser queryParser;
   Query luceneQuery;
   CacheQuery cacheQuery;
   final String key1 = "Navin";
   final String key2 = "BigGoat";
   final String key3 = "MiniGoat";

   public ClusteredCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      // meant to be overridden
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
         .enable()
         .indexLocalOnly(false)
         .addProperty("default.directory_provider", "ram")
         .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   private void prepareTestedObjects() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(30);

      person2 = new Person();
      person2.setName("BigGoat");
      person2.setBlurb("Eats grass");
      person2.setAge(22);

      person3 = new Person();
      person3.setName("MiniGoat");
      person3.setBlurb("Eats cheese");
      person3.setAge(15);
   }

   protected void prepareTestData() throws Exception {
      prepareTestedObjects();

      TransactionManager transactionManager = null;
      transactionManager = cache1.getAdvancedCache().getTransactionManager();

      // Put the 3 created objects in the cache1.

      if (transactionsEnabled()) transactionManager.begin();
      cache1.put(key1, person1);
      cache1.put(key2, person2);
      cache1.put(key3, person3);
      if (transactionsEnabled()) transactionManager.commit();
   }

   protected boolean transactionsEnabled() {
      return false;
   }

   public void testSimple() throws Exception {
      prepareTestData();
      cacheQuery = createCacheQuery(cache2, "blurb", "playing");

      List<Object> found = cacheQuery.list();

      assert found.size() == 1;

      if (found.get(0) == null) {
         log.warn("found.get(0) is null");
         Person p1 = cache2.get(key1);
         if (p1 == null) {
            log.warn("Person p1 is null in sc2 and cannot actually see the data of person1 in sc1");
         } else {
            log.trace("p1 name is  " + p1.getName());

         }
      }

      assert found.get(0).equals(person1);
   }

   private void assertQueryInterceptorPresent(Cache<?, ?> c) {
      CommandInterceptor i = TestingUtil.findInterceptor(c, QueryInterceptor.class);
      assert i != null : "Expected to find a QueryInterceptor, only found "
               + c.getAdvancedCache().getInterceptorChain();
   }

   public void testModified() throws Exception {
      prepareTestData();
      assertQueryInterceptorPresent(cache2);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("playing");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);

      List<Object> found = cacheQuery.list();

      assert found.size() == 1 : "Expected list of size 1, was of size " + found.size();
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache1.put("Navin", person1);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   public void testAdded() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");

      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache1.put("mighty", person4);

      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      found = cacheQuery.list();

      AssertJUnit.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testRemoved() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache1.remove(key3);

      found = cacheQuery.list();
      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "This should not contain object person3 anymore";
   }

   public void testGetResultSize() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("playing");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(1, found.size());
   }

   public void testPutMap() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assert searchManager.getQuery(allQuery, Person.class).list().size() == 3;

      Map<String,Person> allWrites = new HashMap<String,Person>();
      allWrites.put(key1, person1);
      allWrites.put(key2, person2);
      allWrites.put(key3, person3);

      cache2.putAll(allWrites);
      List found = searchManager.getQuery(allQuery, Person.class).list();
      AssertJUnit.assertEquals(3, found.size());

      cache2.putAll(allWrites);
      found = searchManager.getQuery(allQuery, Person.class).list();
      AssertJUnit.assertEquals(3, found.size());
   }

   public void testClear() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache1).getQuery(luceneQuery);

      Query[] queries = new Query[2];
      queries[0] = luceneQuery;

      luceneQuery = queryParser.parse("playing");
      queries[1] = luceneQuery;

      Query luceneQuery = queries[0].combine(queries);
      CacheQuery cacheQuery = Search.getSearchManager(cache1).getQuery(luceneQuery);
      AssertJUnit.assertEquals(3, cacheQuery.getResultSize());

      cache2.clear();

      AssertJUnit.assertEquals(3, cacheQuery.getResultSize());
      cacheQuery = Search.getSearchManager(cache1).getQuery(luceneQuery);

      AssertJUnit.assertEquals(0, cacheQuery.getResultSize());
   }

   public void testFullTextFilterOnOff() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("eats");

      CacheQuery query = Search.getSearchManager(cache1).getQuery(luceneQuery);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "cheese");

      AssertJUnit.assertEquals(1, query.getResultSize());
      List result = query.list();

      Person person = (Person) result.get(0);
      AssertJUnit.assertEquals("MiniGoat", person.getName());
      AssertJUnit.assertEquals("Eats cheese", person.getBlurb());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      AssertJUnit.assertEquals(2, query.getResultSize());
   }

   public void testCombinationOfFilters() throws Exception {
      prepareTestData();

      person4 = new Person();
      person4.setName("ExtraGoat");
      person4.setBlurb("Eats grass and is retired");
      person4.setAge(70);
      cache1.put("ExtraGoat", person4);

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("eats");

      CacheQuery query = Search.getSearchManager(cache1).getQuery(luceneQuery);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "grass");

      AssertJUnit.assertEquals(2, query.getResultSize());

      FullTextFilter ageFilter = query.enableFullTextFilter("personAgeFilter");
      ageFilter.setParameter("age", 70);

      AssertJUnit.assertEquals(1, query.getResultSize());
      List result = query.list();

      Person person = (Person) result.get(0);
      AssertJUnit.assertEquals("ExtraGoat", person.getName());
      AssertJUnit.assertEquals(70, person.getAge());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      query.disableFullTextFilter("personAgeFilter");
      AssertJUnit.assertEquals(3, query.getResultSize());
   }

   public void testSearchKeyTransformer() throws Exception {
      Cache cache1 = caches().get(0);
      Cache cache2 = caches().get(1);

      SearchManager manager = Search.getSearchManager(cache1);
      SearchManager manager1 = Search.getSearchManager(cache2);
      manager.registerKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);
      manager1.registerKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);

      prepareTestedObjects();

      TransactionManager transactionManager = cache1.getAdvancedCache().getTransactionManager();

      CustomKey3 customeKey1 = new CustomKey3(key1);
      CustomKey3 customeKey2 = new CustomKey3(key2);
      CustomKey3 customeKey3 = new CustomKey3(key3);

      // Put the 3 created objects in the cache1.
      if (transactionsEnabled()) transactionManager.begin();
      cache1.put(customeKey1, person1);
      cache1.put(customeKey2, person2);
      cache2.put(customeKey3, person3);
      if (transactionsEnabled()) transactionManager.commit();

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");

      CacheQuery cacheQuery = manager.getQuery(luceneQuery);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));

      int counter = 0;
      while(found.hasNext()) {
         found.next();
         counter++;
      }

      AssertJUnit.assertEquals(2, counter);
   }

}
