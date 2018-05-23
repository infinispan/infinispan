package org.infinispan.query.queries.spatial;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Latitude;
import org.hibernate.search.annotations.Longitude;
import org.hibernate.search.annotations.Spatial;
import org.hibernate.search.annotations.SpatialMode;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.engine.ProjectionConstants;
import org.hibernate.search.query.dsl.QueryBuilder;
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
@Test(groups = {"functional"}, testName = "query.queries.spatial.QuerySpatialTest")
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

      double centerLatitude = 39.51;
      double centerLongitude = -73.5;

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
            .onField("city_location")
            .within(50, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      CacheQuery<?> cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<?> found = cacheQuery.list();

      // found none in 50km
      assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get().spatial()
            .onField("city_location")
            .within(100, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      // found one in 100Km
      assertEquals(1, found.size());

      QueryBuilder builder = Search.getSearchManager(cache).buildQueryBuilderForClass(CitySpatial.class).get();
      query = builder.bool()
            .must(builder.spatial().onField("city_location").within(300, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery())
            .must(builder.spatial().onField("airport_location").within(300, Unit.KM).ofLatitude(centerLatitude).andLongitude(centerLongitude).createQuery())
            .createQuery();

      SortField sortField = new SortField("city_location", SortField.Type.DOUBLE);
      Sort sort = new Sort(sortField);

      Sort distanceSort = builder
            .sort()
            .byDistance().onField("airport_location").fromLatitude(0).andLongitude(0)
            .andByDistance().onField("city_location").fromLatitude(0).andLongitude(0)
            .createSort();

      cacheQuery = Search.getSearchManager(cache).getQuery(query)
            .sort(distanceSort)
            .projection("name", ProjectionConstants.SPATIAL_DISTANCE, ProjectionConstants.SPATIAL_DISTANCE)
            .setSpatialParameters(0, 0, "airport_location")
            .setSpatialParameters(0, 0, "city_location")
      ;
      found = cacheQuery.list();

      // found two in 300Km
      assertEquals(2, found.size());
   }

   private void loadData() {
      cache.put("key1", new CitySpatial("Gotham City", 39.51d, -74.45d, 39.55d, -74.55d));
      cache.put("key2", new CitySpatial("New York City", 40.73d, -73.93d, 40.64, -73.79d));
   }

   //todo try also @Spatial(name = "") to see how an empty property name behaves
   @Indexed
   @Spatial(name = "city_location", spatialMode = SpatialMode.HASH)
   @Spatial(name = "airport_location", spatialMode = SpatialMode.RANGE)
   static public class CitySpatial implements Coordinates {

      // the location of the center of the city
      private final Double latitude;
      private final Double longitude;

      // the location of the airport of the city
      private final Double airportLatitude;
      private final Double airportLongitude;

      @Field(store = Store.YES)
      String name;

      public CitySpatial(String name, Double latitude, Double longitude, Double airportLatitude, Double airportLongitude) {
         this.name = name;
         this.latitude = latitude;
         this.longitude = longitude;
         this.airportLatitude = airportLatitude;
         this.airportLongitude = airportLongitude;
      }

      @Override
      public Double getLatitude() {
         return latitude;
      }

      @Override
      public Double getLongitude() {
         return longitude;
      }

      @Latitude(of = "airport_location")
      public Double getAirportLatitude() {
         return latitude;
      }

      @Longitude(of = "airport_location")
      public Double getAirportLongitude() {
         return longitude;
      }
   }
}
