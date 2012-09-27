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

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.engine.spi.EntityIndexBinder;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.Search;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;
import org.testng.annotations.Test;

import static java.util.Arrays.asList;
import static org.infinispan.query.helper.TestQueryHelperFactory.*;

@Test(groups = "functional", testName = "query.blackbox.LocalCacheTest")
public class LocalCacheTest extends SingleCacheManagerTest {
   protected Person person1;
   protected Person person2;
   protected Person person3;
   protected Person person4;
   protected Person person5;
   protected AnotherGrassEater anotherGrassEater;
   protected QueryParser queryParser;
   protected String key1 = "Navin";
   protected String key2 = "BigGoat";
   protected String key3 = "MiniGoat";
   protected String anotherGrassEaterKey = "anotherGrassEaterKey";
   
   public LocalCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSimple() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      List<Object> found = cacheQuery.list();

      int elems = found.size();
      assert elems == 1 : "Expected 1 but was " + elems;

      Object val = found.get(0);
      assert val.equals(person1) : "Expected " + person1 + " but was " + val;
   }

   public void testEagerIterator() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      QueryIterator found = cacheQuery.iterator(new FetchOptions(FetchOptions.FetchMode.EAGER));

      assert found.hasNext();
      found.next();
      assert !found.hasNext();
   }

   public void testMultipleResults() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0) == person2;
      assert found.get(1) == person3;

   }

   public void testModified() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      List<Object> found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   public void testAdded() throws ParseException {
      assertIndexingKnows(cache);
      loadTestingData();
      assertIndexingKnows(cache, Person.class, AnotherGrassEater.class);
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testRemoved() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache.remove(key3);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "The search should not return person3";
   }

   public void testUpdated() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2) : "The search should have person2";

      cache.put(key2, person1);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();
      
      assert found.size() == 1 : "Size of list should be 1";
      assert !found.contains(person2) : "Person 2 should not be found now";
      assert !found.contains(person1) : "Person 1 should not be found because it does not meet the search criteria";
   }

   public void testSetSort() throws ParseException {
      loadTestingData();
      person2.setAge(35);
      person3.setAge(12);

      Sort sort = new Sort( new SortField("age", SortField.STRING));

      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery.sort(sort);

      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0).equals(person2);
      assert found.get(1).equals(person3);
   }

   public void testSetFilter() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;

      Filter filter = new PrefixFilter(new Term("blurb", "cheese"));

      cacheQuery.filter(filter);

      found = cacheQuery.list();

      assert found.size() == 1;
   }

   public void testLazyIterator() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      QueryIterator found = cacheQuery.iterator(new FetchOptions(FetchOptions.FetchMode.LAZY));

      assert found.hasNext();
      found.next();
      assert !found.hasNext();
   }

   public void testGetResultSize() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 1;
   }

   public void testClear() {
      loadTestingData();

      // Create a term that will return me everyone called Navin.
      Term navin = new Term("name", "navin");

      // Create a term that I know will return me everything with name goat.
      Term goat = new Term ("name", "goat");

      Query[] queries = new Query[2];
      queries[0] = new TermQuery(goat);
      queries[1] = new TermQuery(navin);

      Query luceneQuery = queries[0].combine(queries);
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      // We know that we've got all 3 hits.
      assert cacheQuery.getResultSize() == 3 : "Expected 3, got " + cacheQuery.getResultSize();

      cache.clear();

      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 0;
   }

   public void testTypeFiltering() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("grass");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.containsAll(asList(person2, anotherGrassEater));

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("grass");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, AnotherGrassEater.class);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(anotherGrassEater);
   }
   
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
   
   public void testEntityDiscovery() {
      assertIndexingKnows(cache);

      Person p = new Person();
      p.setName("Lucene developer");
      p.setAge(30);
      p.setBlurb("works best on weekends");
      cache.put(p.getName(), p);
      
      assertIndexingKnows(cache, Person.class);
   }

   /**
    * Verifies if the indexing interceptor is aware of a specific list of types.
    * @param cache the cache containing the indexes
    * @param types vararg listing the types the indexing engine should know
    */
   private void assertIndexingKnows(Cache<Object, Object> cache, Class... types) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      SearchFactoryImplementor searchFactoryIntegrator = (SearchFactoryImplementor) cr.getComponent(SearchFactoryIntegrator.class);
      Assert.assertNotNull(searchFactoryIntegrator);
      Map<Class<?>, EntityIndexBinder> indexBindingForEntity = searchFactoryIntegrator.getIndexBindingForEntity();
      Assert.assertNotNull(indexBindingForEntity);
      Set<Class<?>> keySet = indexBindingForEntity.keySet();
      Assert.assertEquals(types.length, keySet.size());
      Assert.assertTrue(keySet.containsAll(Arrays.asList(types)));
   }

   protected void loadTestingData() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");

      person5 = new Person();
      person5.setName("Smelly Cat");
      person5.setBlurb("Eats fish");

      anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");

      cache.put(key1, person1);
      
      // person2 is verified as number of matches in multiple tests,
      // so verify duplicate insertion doesn't affect result counts:
      cache.put(key2, person2);
      cache.put(key2, person2);
      cache.put(key2, person2);
      
      cache.put(key3, person3);
      cache.put(anotherGrassEaterKey, anotherGrassEater);
   }
   
   protected void enhanceConfig(ConfigurationBuilder c) {
      // no op, meant to be overridden
   }

}
