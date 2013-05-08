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

package org.infinispan.query.queries.faceting;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.facet.Facet;
import org.hibernate.search.query.facet.FacetingRequest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Hardy Ferentschik
 */
@Test(groups = "functional", testName = "query.queries.faceting.SimpleFacetingTest")
public class SimpleFacetingTest extends SingleCacheManagerTest {
   
   private static final String indexFieldName = "cubicCapacity";
   private static final String facetName = "ccs";
   
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
      cache.put( "195 Inter", new Car( "Ferrari 195 Inter", "Rosso corsa", 2341 ) );
      cache.put( "212 Inter", new Car( "Ferrari 212 Inter", "black", 4000 ) );
      cache.put( "500_Superfast", new Car( "Ferrari 500_Superfast", "Rosso corsa", 4000 ) );
      //test for duplication:
      cache.put( "500_Superfast", new Car( "Ferrari 500_Superfast", "Rosso corsa", 4000 ) );
   }
   
   @AfterMethod
   public void cleanupData() {
      cache.clear();
   }
   
   public void testFaceting() throws Exception {
      QueryBuilder queryBuilder = qf.buildQueryBuilderForClass( Car.class ).get();
      
      FacetingRequest request = queryBuilder.facet()
            .name( facetName )
            .onField( indexFieldName )
            .discrete()
            .createFacetingRequest();
      
      Query luceneQuery = queryBuilder.all().createQuery();
      
      CacheQuery query = qf.getQuery(luceneQuery);

      query.getFacetManager().enableFaceting( request );

      List<Facet> facetList = query.getFacetManager().getFacets( facetName );
      
      assertEquals("Wrong number of facets", 2, facetList.size());
      
      assertEquals("4000", facetList.get(0).getValue());
      assertEquals(2, facetList.get(0).getCount());
      assertEquals(indexFieldName, facetList.get(0).getFieldName());
      
      assertEquals("2341", facetList.get(1).getValue());
      assertEquals(1, facetList.get(1).getCount());
      assertEquals(indexFieldName, facetList.get(1).getFieldName());
   }

}
