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
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * TODO [anistor]
 *  - spatial query not available in unindexed mode, with listeners or CQ ?? (not technically impossible)
 *  - spatial query not avaialble in queries with aggregation ?? (not technically impossible)
 *  - geodist available for SELECT and ORDER BY
 *  - geofilt available for WHERE only
 */

/**
 * Testing and verifying that Spatial queries work properly, with the Hibernate Search API and also with Ickle query
 * string.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 */
@Test(groups = {"functional"}, testName = "query.queries.spatial.QuerySpatialTest")
public class QuerySpatialTest extends SingleCacheManagerTest {

   public QuerySpatialTest() {
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
            .setSpatialParameters(0, 0, "city_location");

      found = cacheQuery.list();

      // found two in 300Km
      assertEquals(2, found.size());
   }

   public void testIckleSpatialQueries() {
      double centerLatitude = 39.51;
      double centerLongitude = -73.5;

      double distance = 50;
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      org.infinispan.query.dsl.Query query = queryFactory
            .create("from " + CitySpatial.class.getName() + " where geofilt(city_location, " + centerLatitude + ", " + centerLongitude + ", " + distance + ")");
      List<?> found = query.list();

      // found none in 50km
      assertEquals(0, found.size());

      distance = 100;
      query = queryFactory
            .create("from " + CitySpatial.class.getName() + " where geofilt(city_location, " + centerLatitude + ", " + centerLongitude + ", " + distance + ")");
      found = query.list();

      // found one in 100Km
      assertEquals(1, found.size());

      distance = 300;
      query = queryFactory
            .create("select name"
                  + ", geodist(city_location, 0, 0), geodist(airport_location, 0, 0) "
                  + " from " + CitySpatial.class.getName() + " where geofilt(city_location, " + centerLatitude + ", " + centerLongitude + ", " + distance + ")"
                  + " and geofilt(city_location, " + centerLatitude + ", " + centerLongitude + ", " + distance + ")"
                  + " order by geodist(airport_location, 0, 0), geodist(city_location, 0, 0)"
                  + "");
      found = query.list();

      // found two in 300Km
      assertEquals(2, found.size());
   }

   @BeforeClass(alwaysRun = true)
   protected void loadData() {
      cache.put("key1", new CitySpatial("Gotham City", 39.51d, -74.45d, 39.55d, -74.55d));
      cache.put("key2", new CitySpatial("New York City", 40.73d, -73.93d, 40.64, -73.79d));
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() {
      // no need to clear cache between tests
   }

   //todo [anistor] try also @Spatial(name = "") to see how an empty property name behaves
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

      CitySpatial(String name, Double latitude, Double longitude, Double airportLatitude, Double airportLongitude) {
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
