/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.query.indexedembedded;

import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.indexedembedded.CollectionsIndexingTest")
public class CollectionsIndexingTest extends SingleCacheManagerTest {

   private SearchManager qf;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
         .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeClass
   public void prepareSearchFactory() throws Exception {
      qf = Search.getSearchManager(cache);
   }
   
   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }
   
   @Test
   public void searchOnEmptyIndex() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( 0 , list.size() );
   }
   
   @Test
   public void searchOnAllTypes() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( 1 , list.size() );
      list = qf.getQuery(query).list();
      Assert.assertEquals( 1 , list.size() );
      list = qf.getQuery( new MatchAllDocsQuery() ).list();
      Assert.assertEquals( 1 , list.size() );
   }
   
   @Test
   public void searchOnSimpleField() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("countryName");
      Query query = queryParser.parse("Italy");
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( 1 , list.size() );
   }
   
   @Test
   public void searchOnEmbeddedField() throws ParseException {
      QueryParser queryParser = TestQueryHelperFactory.createQueryParser("cities.name");
      Query query = queryParser.parse("Newcastle");
      
      Country uk = new Country();
      City london = new City();
      london.name = "London";
      City newcastle = new City();
      newcastle.name = "Newcastle";
      uk.countryName = "United Kingdom";
      uk.cities.add(newcastle);
      uk.cities.add(london);
      
      //verify behaviour on multiple insertions as well:
      cache.put("UK", uk);
      cache.put("UK", uk);
      cache.put("UK", uk);
      List<Object> list = qf.getQuery(query, Country.class, City.class).list();
      Assert.assertEquals( 1 , list.size() );
      Assert.assertTrue( uk == list.get(0) );
   }

}
