/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.AfterMethod;

import java.util.List;

public abstract class AbstractLocalQueryTest extends SingleCacheManagerTest {
   protected Person person1;
   protected Person person2;
   protected Person person3;
   protected Person person4;
   protected Person person5;
   protected Person person6;
   protected QueryParser queryParser;
   protected Query luceneQuery;
   protected CacheQuery cacheQuery;
   protected List found;
   protected String key1 = "Navin";
   protected String key2 = "BigGoat";
   protected String key3 = "MiniGoat";

   protected Cache<Object, Person> cache;
   protected QueryHelper qh;

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      if (cache != null) cache.stop();
   }

   public void testSimple() throws ParseException {
      cacheQuery = new QueryFactory(cache, qh).getBasicQuery("blurb", "playing");

      found = cacheQuery.list();

      int elems = found.size();
      assert elems == 1 : "Expected 1 but was " + elems;

      Object val = found.get(0);
      assert val.equals(person1) : "Expected " + person1 + " but was " + val;
   }

   public void testEagerIterator() throws ParseException {

      cacheQuery = new QueryFactory(cache, qh).
            getBasicQuery("blurb", "playing");

      QueryIterator found = cacheQuery.iterator();

      assert found.isFirst();
      assert found.isLast();
   }

   public void testMultipleResults() throws ParseException {

      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0) == person2;
      assert found.get(1) == person3;

   }

   public void testModified() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   public void testAdded() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testRemoved() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache.remove(key3);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      System.out.println("found is: - " + found);
      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "The search should not return person3";


   }

   public void testSetSort() throws ParseException {
      person2.setAge(35);
      person3.setAge(12);

      Sort sort = new Sort("age");

      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery.setSort(sort);

      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0).equals(person2);
      assert found.get(1).equals(person3);
   }

   public void testSetFilter() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;

      Filter filter = new PrefixFilter(new Term("blurb", "cheese"));

      cacheQuery.setFilter(filter);

      found = cacheQuery.list();

      assert found.size() == 1;

   }

   public void testLazyIterator() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      QueryIterator found = cacheQuery.lazyIterator();

      assert found.isFirst();
      assert found.isLast();

   }

   public void testGetResultSize() throws ParseException {

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 1;
   }

   public void testClear() throws ParseException{

      // Create a term that will return me everyone called Navin.
      Term navin = new Term("name", "navin");

      // Create a term that I know will return me everything with name goat.
      Term goat = new Term ("name", "goat");

      Query[] queries = new Query[2];
      queries[0] = new TermQuery(goat);
      queries[1] = new TermQuery(navin);

      luceneQuery = queries[0].combine(queries);
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      // We know that we've got all 3 hits.
      assert cacheQuery.getResultSize() == 3;

      cache.clear();

      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 0;
   }
}
