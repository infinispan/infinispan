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

import junit.framework.Assert;

import org.apache.lucene.queryParser.ParseException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.QueryFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class CollectionsIndexingTest extends SingleCacheManagerTest {

   private QueryFactory qf;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultStandaloneConfig(true);
      c.fluent()
         .indexing()
         .indexLocalOnly(false)
         .addProperty("hibernate.search.default.directory_provider", "ram");
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

   @BeforeClass
   public void prepareSearchFactory() throws Exception {
      qf = new QueryFactory(cache);
   }
   
   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }
   
   @Test
   public void searchOnEmptyIndex() throws ParseException {
      List<Object> list = qf.getBasicQuery("countryName", "Italy", Country.class, City.class).list();
      Assert.assertEquals( 0 , list.size() );
   }
   
   @Test
   public void searchOnSimpleField() throws ParseException {
      Country italy = new Country();
      italy.countryName = "Italy";
      cache.put("IT", italy);
      List<Object> list = qf.getBasicQuery("countryName", "Italy", Country.class, City.class).list();
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
      List<Object> list = qf.getBasicQuery("cities.name", "Newcastle", Country.class, City.class).list();
      Assert.assertEquals( 1 , list.size() );
      Assert.assertTrue( uk == list.get(0) );
   }

}
