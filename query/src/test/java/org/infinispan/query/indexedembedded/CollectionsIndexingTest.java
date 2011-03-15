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

package org.infinispan.query.indexedembedded;

import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.lucene.queryParser.ParseException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class CollectionsIndexingTest extends SingleCacheManagerTest {

   private QueryHelper qh;
   private QueryFactory qf;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultStandaloneConfig(true);
      c.configureIndexing().enabled(true).indexLocalOnly(true);
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

   @BeforeClass
   public void prepareSearchFactory() throws Exception {
      Properties p = new Properties();
      p.setProperty("hibernate.search.default.directory_provider", "org.hibernate.search.store.RAMDirectoryProvider");
      qh = new QueryHelper(cache, p, City.class, Country.class);
      qf = new QueryFactory(cache, qh);
   }
   
   @AfterClass
   public void closeSearchFactory() {
      qh.close();
   }
   
   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }
   
   @Test
   public void searchOnEmptyIndex() throws ParseException {
      List<Object> list = qf.getBasicQuery("countryName", "Italy", TestQueryHelperFactory.getLuceneVersion()).list();
      Assert.assertEquals( 0 , list.size() );
   }
   
   @Test
   public void searchOnSimpleField() throws ParseException {
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getBasicQuery("countryName", "Italy", TestQueryHelperFactory.getLuceneVersion()).list();
      Assert.assertEquals( 1 , list.size() );
   }
   
   @Test
   public void searchOnEmbeddedField() throws ParseException {
      Country uk = new Country();
      City london = new City();
      london.name = "London";
      City newcastle = new City();
      newcastle.name = "Newcastle";
      uk.countryName = "United Kingdom";
      uk.cities.add(newcastle);
      uk.cities.add(london);
      cache.put("UK", uk);
      List<Object> list = qf.getBasicQuery("cities.name", "Newcastle", TestQueryHelperFactory.getLuceneVersion()).list();
      Assert.assertEquals( 1 , list.size() );
      Assert.assertTrue( uk == list.get(0) );
   }

}
