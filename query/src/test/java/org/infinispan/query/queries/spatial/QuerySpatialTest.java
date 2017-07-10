package org.infinispan.query.queries.spatial;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Spatial;
import org.hibernate.search.annotations.SpatialMode;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.query.dsl.Unit;
import org.hibernate.search.spatial.Coordinates;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing and verifying that Spatial queries work properly. The information on the coordinates was taken from the
 * hibernate-search tests.
 *
 * @author Anna Manukyan
 */
@Test(groups = {"functional", "smoke"}, testName = "query.queries.spatial.QuerySpatialTest")
public class QuerySpatialTest extends SingleCacheManagerTest {

   public QuerySpatialTest() {
      cleanup = AbstractCacheTest.CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(CitySpatial.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testSpatialQueries() {
      loadData();

      double centerLatitude = 24;
      double centerLongitude = 31.5;

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
         .onField("city_location")
         .within(50, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      CacheQuery<?> cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<?> found = cacheQuery.list();

      assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
            .onField("city_location")
            .within(51, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assertEquals(1, found.size());
   }

   private void loadData() {
      CitySpatial city1 = new CitySpatial(24.0d, 32.0d, "Some City");
      cache.put("key1", city1);
   }

   @Indexed
   @Spatial(name = "city_location", spatialMode = SpatialMode.HASH)
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
