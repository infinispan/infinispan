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
import org.apache.lucene.search.TermQuery;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.test.CustomKey;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.config.Configuration.CacheMode.LOCAL;

/**
 * Class that will put in different kinds of keys into the cache and run a query on it to see if
 * different primitives will work as keys.
 *
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class KeyTypeTest extends SingleCacheManagerTest {

   Person person1;

   public KeyTypeTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultClusteredConfig(LOCAL, true);
      c.fluent()
         .indexing()
         .indexLocalOnly(false)
         .addProperty("hibernate.search.default.directory_provider", "ram")
         .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      cacheManager = TestCacheManagerFactory.createCacheManager(c, true);

      person1 = new Person();
      person1.setName("Navin");
      person1.setBlurb("Owns a macbook");
      person1.setAge(20);
      return cacheManager;
   }

   public void testPrimitiveAndStringKeys(){
      String key1 = "key1";
      int key2 = 2;
      byte key3 = 3;
      float key4 = 4;
      long key5 = 5;
      short key6 = 6;
      boolean key7 = true;
      double key8 = 8;
      char key9 = '9';


      cache.put(key1, person1);
      cache.put(key2, person1);
      cache.put(key3, person1);
      cache.put(key4, person1);
      cache.put(key5, person1);
      cache.put(key6, person1);
      cache.put(key7, person1);
      cache.put(key8, person1);
      cache.put(key9, person1);

      // Going to search the 'blurb' field for 'owns'
      Term term = new Term ("blurb", "owns");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(new TermQuery(term));
      assert cacheQuery.getResultSize() == 9;

      List<Object> found = cacheQuery.list();
      for (int i = 0; i < 9; i++){
         assert found.get(i).equals(person1);
      }

   }

   public void testCustomKeys(){
      CustomKey key1 = new CustomKey(1, 2, 3);
      CustomKey key2 = new CustomKey(900, 800, 700);
      CustomKey key3 = new CustomKey(1024, 2048, 4096);

      cache.put(key1, person1);
      cache.put(key2, person1);
      cache.put(key3, person1);

      Term term = new Term("blurb", "owns");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(new TermQuery(term));
      int i;
      assert (i = cacheQuery.getResultSize()) == 3 : "Expected 3, was " + i;
   }
}
