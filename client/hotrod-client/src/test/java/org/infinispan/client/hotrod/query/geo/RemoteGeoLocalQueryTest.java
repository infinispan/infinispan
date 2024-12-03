package org.infinispan.client.hotrod.query.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.api.query.geo.LatLng;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.model.Restaurant;
import org.infinispan.query.model.TrainRoute;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.geo.RemoteGeoLocalQueryTest")
public class RemoteGeoLocalQueryTest extends SingleHotRodServerTest {

   private static final String RESTAURANT_ENTITY_NAME = "geo.Restaurant";
   private static final String TRAIN_ROUTE_ENTITY_NAME = "geo.TrainRoute";

   private static final LatLng MILAN_COORDINATES = LatLng.of(45.4685, 9.1824);
   private static final LatLng COMO_COORDINATES = LatLng.of(45.8064, 9.0852);
   private static final LatLng BOLOGNA_COORDINATES = LatLng.of(44.4949, 11.3426);
   private static final LatLng ROME_COORDINATES = LatLng.of(41.8967, 12.4822);
   private static final LatLng VENICE_COORDINATES = LatLng.of(45.4404, 12.3160);
   private static final LatLng SELVA_COORDINATES = LatLng.of(46.5560, 11.7559);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(RESTAURANT_ENTITY_NAME)
            .addIndexedEntity(TRAIN_ROUTE_ENTITY_NAME);
      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Restaurant.RestaurantSchema.INSTANCE;
   }

   @Test
   public void verifySpatialMapping() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();

      assertThat(metamodel).containsKeys(RESTAURANT_ENTITY_NAME);
      IndexMetamodel indexMetamodel = metamodel.get(RESTAURANT_ENTITY_NAME);
      assertThat(indexMetamodel.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "description", "address", "location", "score");

      assertThat(metamodel).containsKeys(TRAIN_ROUTE_ENTITY_NAME);
      indexMetamodel = metamodel.get(TRAIN_ROUTE_ENTITY_NAME);
      assertThat(indexMetamodel.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "departure", "arrival");
   }

   @Test
   public void indexingAndSearch() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      // data taken from https://www.google.com/maps/
      remoteCache.put("La Locanda di Pietro", new Restaurant("La Locanda di Pietro",
            "Roman-style pasta dishes & Lazio region wines at a cozy traditional trattoria with a shaded terrace.",
            "Via Sebastiano Veniero, 28/c, 00192 Roma RM", 41.907903484609356, 12.45540543756422, 4.6f));
      remoteCache.put("Scialla The Original Street Food", new Restaurant("Scialla The Original Street Food",
            "Pastas & traditional pizza pies served in an unassuming eatery with vegetarian options.",
            "Vicolo del Farinone, 27, 00193 Roma RM", 41.90369455835456, 12.459566517195528, 4.7f));
      remoteCache.put("Trattoria Pizzeria Gli Archi", new Restaurant("Trattoria Pizzeria Gli Archi",
            "Traditional trattoria with exposed brick walls, serving up antipasti, pizzas & pasta dishes.",
            "Via Sebastiano Veniero, 26, 00192 Roma RM", 41.907930453801285, 12.455204785977637, 4.0f));
      remoteCache.put("Alla Bracioleria Gracchi Restaurant", new Restaurant("Alla Bracioleria Gracchi Restaurant",
            "", "Via dei Gracchi, 19, 00192 Roma RM", 41.907129402661795, 12.458927251586584, 4.7f ));
      remoteCache.put("Magazzino Scipioni", new Restaurant("Magazzino Scipioni",
            "Contemporary venue with a focus on unique wines & seasonal Italian plates, plus a bottle shop.",
            "Via degli Scipioni, 30, 00192 Roma RM", 41.90817843995448, 12.457118458698043, 4.6f));
      remoteCache.put("Dal Toscano Restaurant", new Restaurant("Dal Toscano Restaurant",
            "Rich pastas, signature steaks & classic Tuscan dishes, plus Chianti wines, at a venerable trattoria.",
            "Via Germanico, 58-60, 00192 Roma RM", 41.90785274056548, 12.45822050287784, 4.2f));
      remoteCache.put("Il Ciociaro", new Restaurant("Il Ciociaro",
            "Long-running, old-school restaurant plating traditional staples, from carbonara to tiramisu.",
            "Via Barletta, 21, 00192 Roma RM", 41.91038657525997, 12.458851939120656, 4.2f));

      String ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      Query<Restaurant> query = remoteCache.query(ickle);
      query.setParameter("distance", 100);
      List<Restaurant> list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 150m) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 0.15 km) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 0.0932057mi) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance yd) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 164.042);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance nmi) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 0.0809935);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 250);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant");

      ickle = String.format("from %s r " +
            "where r.location within box(41.91, 12.45, 41.90, 12.46)", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Scialla The Original Street Food", "Alla Bracioleria Gracchi Restaurant");

      ickle = String.format("from %s r where r.location within" +
            " polygon((41.91, 12.45), (41.91, 12.46), (41.90, 12.46), (41.90, 12.46))", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Scialla The Original Street Food", "Alla Bracioleria Gracchi Restaurant");

      ickle = String.format("select distance(r.location, 41.90847031512531, 12.455633288333539) from %s r", RESTAURANT_ENTITY_NAME);
      Query<Object[]> projectQuery = remoteCache.query(ickle);
      List<Object[]> projectList = projectQuery.list();
      assertThat(projectList).extracting(item -> item[0])
            .containsExactlyInAnyOrder(65.78997502576355, 622.8579549605669, 69.72458363789359, 310.6984480274634,
                  127.11531555461053, 224.8438726836208, 341.0897945700656);

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = remoteCache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(65.78997502576355)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(622.8579549605669)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("from %s r order by distance(r.location, 41.90847031512531, 12.455633288333539)", RESTAURANT_ENTITY_NAME);
      query = remoteCache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactly("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Alla Bracioleria Gracchi Restaurant", "Il Ciociaro",
                  "Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539) from %s r " +
            "order by distance(r.location, 41.90847031512531, 12.455633288333539)", RESTAURANT_ENTITY_NAME);
      projectQuery = remoteCache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList).extracting(item -> item[0])
            .containsExactly("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Alla Bracioleria Gracchi Restaurant", "Il Ciociaro",
                  "Scialla The Original Street Food");
      assertThat(projectList).extracting(item -> item[1])
            .containsExactly(65.78997502576355, 69.72458363789359, 127.11531555461053, 224.8438726836208,
                  310.6984480274634, 341.0897945700656, 622.8579549605669);

   }

   @Test
   public void indexingAndSearch_multiGeoPointEntities() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      remoteCache.put("Rome-Milan", new TrainRoute("Rome-Milan", ROME_COORDINATES.latitude(), ROME_COORDINATES.longitude(),
            MILAN_COORDINATES.latitude(), MILAN_COORDINATES.longitude()));
      remoteCache.put("Bologna-Selva", new TrainRoute("Bologna-Selva", BOLOGNA_COORDINATES.latitude(), BOLOGNA_COORDINATES.longitude(),
            SELVA_COORDINATES.latitude(), SELVA_COORDINATES.longitude()));
      remoteCache.put("Milan-Como", new TrainRoute("Milan-Como", MILAN_COORDINATES.latitude(), MILAN_COORDINATES.longitude(),
            COMO_COORDINATES.latitude(), COMO_COORDINATES.longitude()));
      remoteCache.put("Bologna-Venice", new TrainRoute("Bologna-Venice", BOLOGNA_COORDINATES.latitude(), BOLOGNA_COORDINATES.longitude(),
            VENICE_COORDINATES.latitude(), VENICE_COORDINATES.longitude()));

      Query<TrainRoute> trainQuery = remoteCache.query("from geo.TrainRoute r where r.departure within circle(:lat, :lon, :distance)");
      trainQuery.setParameter("lat", BOLOGNA_COORDINATES.latitude());
      trainQuery.setParameter("lon", BOLOGNA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 300_000);
      List<TrainRoute> trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Venice", "Bologna-Selva");

      trainQuery = remoteCache.query("from geo.TrainRoute r where r.arrival within circle(:lat, :lon, :distance)");
      trainQuery.setParameter("lat", SELVA_COORDINATES.latitude());
      trainQuery.setParameter("lon", SELVA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 200_000);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Bologna-Venice", "Bologna-Selva");

      trainQuery = remoteCache.query("from geo.TrainRoute r where r.arrival within box(:a, :b, :c, :d)");
      trainQuery.setParameter("a", 47.00);
      trainQuery.setParameter("b", 8.00);
      trainQuery.setParameter("c", 45.70);
      trainQuery.setParameter("d", 12.00);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Selva");

      trainQuery = remoteCache.query("from geo.TrainRoute r where r.arrival within polygon(:a, :b, :c, :d)");
      trainQuery.setParameter("a", "(47.00, 8.00)");
      trainQuery.setParameter("b", "(47.00, 12.00)");
      trainQuery.setParameter("c", "(45.70, 12.00)");
      trainQuery.setParameter("d", "(45.70, 8.00)");
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Selva");

      Query<Object[]> query = remoteCache.query("select r.name, distance(r.arrival, 41.90847031512531, 12.455633288333539) " +
            "from geo.TrainRoute r where r.departure within box(45.743465, 8.305000, 44.218980, 12.585290) " +
            "order by distance(r.arrival, 41.90847031512531, 12.455633288333539)");
      QueryResult<Object[]> result = query.execute();
      assertThat(result.count().value()).isEqualTo(3);
      List<Object[]> list = result.list();
      assertThat(list).extracting(item -> item[0]).containsExactly("Bologna-Venice", "Milan-Como", "Bologna-Selva");
      assertThat(list).extracting(item -> item[1]).containsExactly(392893.53564872313, 510660.6643083735, 519774.5486163137);

      query = remoteCache.query("select r.name, distance(r.departure, 41.90847031512531, 12.455633288333539) " +
            "from geo.TrainRoute r where r.arrival within box(45.743465, 8.305000, 44.218980, 12.585290) " +
            "order by distance(r.departure, 41.90847031512531, 12.455633288333539)");
      result = query.execute();
      assertThat(result.count().value()).isEqualTo(2);
      list = result.list();
      assertThat(list).extracting(item -> item[0]).containsExactly("Rome-Milan", "Bologna-Venice");
      assertThat(list).extracting(item -> item[1]).containsExactly(2558.7323262164573, 301408.00282977184);
   }
}
