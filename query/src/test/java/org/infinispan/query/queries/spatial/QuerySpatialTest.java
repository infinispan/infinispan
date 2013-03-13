/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.query.queries.spatial;

import junit.framework.Assert;
import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Spatial;
import org.hibernate.search.annotations.SpatialMode;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.Unit;
import org.hibernate.search.spatial.Coordinates;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Testing and verifying that Spatial queries work properly. The information on the coordinates was taken from the
 * hibernate-search tests.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.queries.spatial.QuerySpatialTest")
public class QuerySpatialTest extends SingleCacheManagerTest {

   public QuerySpatialTest() {
      cleanup = AbstractCacheTest.CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing()
         .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testSpatialQueries() {
      loadData();

      double centerLatitude = 24;
      double centerLongitude = 31.5;

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
         .onCoordinates("city_location")
         .within(50, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
            .onCoordinates("city_location")
            .within(51, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
   }

   private void loadData() {
      CitySpatial city1 = new CitySpatial(24.0d, 32.0d, "Some City");
      cache.put("key1", city1);
   }

   @Indexed
   @Spatial(name = "city_location", spatialMode = SpatialMode.GRID)
   static public class CitySpatial implements Coordinates {

      private Double latitude;
      private Double longitude;

      @Field(store = Store.YES)
      String name;

      public CitySpatial(Double latitude, Double longitude, String name) {
         this.latitude = latitude;
         this.longitude = longitude;
         this.name = name;
      }

      public Double getLatitude() {
         return latitude;
      }

      public Double getLongitude() {
         return longitude;
      }
   }
}
