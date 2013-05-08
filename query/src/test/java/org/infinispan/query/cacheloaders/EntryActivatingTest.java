/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.query.cacheloaders;

import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.indexedembedded.City;
import org.infinispan.query.indexedembedded.Country;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.cacheloaders.EntryActivatingTest")
public class EntryActivatingTest extends AbstractInfinispanTest {

   Cache<String, Country> cache;
   CacheStore store;
   CacheContainer cm;
   SearchManager search;
   QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");

   @BeforeTest
   public void setUp() {
      recreateCacheManager();
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
   }

   public void testPersistence() throws CacheLoaderException, ParseException {
      verifyFullTextHasMatches(0);

      Country italy = new Country();
      italy.countryName = "Italy";
      City rome = new City();
      rome.name = "Rome";
      italy.cities.add(rome);

      cache.put("IT", italy);
      assert ! store.containsKey("IT");

      verifyFullTextHasMatches(1);

      cache.evict("IT");
      assert store.containsKey("IT");

      InternalCacheEntry internalCacheEntry = cache.getAdvancedCache().getDataContainer().get("IT");
      assert internalCacheEntry==null;

      verifyFullTextHasMatches(1);

      Country country = cache.get("IT");
      assert country != null;
      assert "Italy".equals(country.countryName);

      verifyFullTextHasMatches(1);

      cache.stop();
      assert ((SearchFactoryIntegrator)search.getSearchFactory()).isStopped();
      TestingUtil.killCacheManagers(cm);
      
      // Now let's check the entry is not re-indexed during data preloading:
      recreateCacheManager();
      
      // People should generally use a persistent index; we use RAMDirectory for
      // test cleanup, so for our configuration it needs now to contain zero
      // matches: on filesystem it would be exactly one as expected (two when ISPN-1179 was open)
      verifyFullTextHasMatches(0);
   }

   private void recreateCacheManager() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.loaders()
            .preload(true)
            .passivation(true)
            .addStore()
            .cacheStore(new DummyInMemoryCacheStore())
            .purgeOnStartup(true)
         .indexing()
            .enable()
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT")
         ;
      cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class)
            .getCacheStore();
      search = Search.getSearchManager(cache);
   }

   private void verifyFullTextHasMatches(int i) throws ParseException {
      Query query = queryParser.parse("Italy");
      List<Object> list = search.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( i , list.size() );
   }

}
