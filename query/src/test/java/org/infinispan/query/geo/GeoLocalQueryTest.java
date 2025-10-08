package org.infinispan.query.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.api.annotations.indexing.model.LatLng;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Hiking;
import org.infinispan.query.model.Restaurant;
import org.infinispan.query.model.TrainRoute;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.geo.GeoLocalQueryTest")
public class GeoLocalQueryTest extends SingleCacheManagerTest {

   private static final String RESTAURANT_ENTITY_NAME = Restaurant.class.getName();
   private static final String HIKING_ENTITY_NAME = Hiking.class.getName();
   private static final String TRAIN_ROUTE_ENTITY_NAME = TrainRoute.class.getName();

   private static final LatLng MILAN_COORDINATES = LatLng.of(45.4685, 9.1824);
   private static final LatLng COMO_COORDINATES = LatLng.of(45.8064, 9.0852);
   private static final LatLng BOLOGNA_COORDINATES = LatLng.of(44.4949, 11.3426);
   private static final LatLng ROME_COORDINATES = LatLng.of(41.8967, 12.4822);
   private static final LatLng VENICE_COORDINATES = LatLng.of(45.4404, 12.3160);
   private static final LatLng SELVA_COORDINATES = LatLng.of(46.5560, 11.7559);

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Restaurant.class)
            .addIndexedEntity(Hiking.class)
            .addIndexedEntity(TrainRoute.class);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test
   public void verifySpatialMapping() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();

      assertThat(metamodel).containsKeys(RESTAURANT_ENTITY_NAME);
      IndexMetamodel restaurantMeta = metamodel.get(RESTAURANT_ENTITY_NAME);
      assertThat(restaurantMeta.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "description", "address", "location", "score");

      assertThat(metamodel).containsKeys(HIKING_ENTITY_NAME);
      IndexMetamodel hikingMeta = metamodel.get(HIKING_ENTITY_NAME);
      assertThat(hikingMeta.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "start", "end");

      assertThat(metamodel).containsKeys(TRAIN_ROUTE_ENTITY_NAME);
      IndexMetamodel trainMeta = metamodel.get(TRAIN_ROUTE_ENTITY_NAME);
      assertThat(trainMeta.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "departure", "arrival");
   }

   @Test
   public void indexingAndSearch() {
      // data taken from https://www.google.com/maps/
      cache.put("La Locanda di Pietro", new Restaurant("La Locanda di Pietro",
            "Roman-style pasta dishes & Lazio region wines at a cozy traditional trattoria with a shaded terrace.",
            "Via Sebastiano Veniero, 28/c, 00192 Roma RM", 41.907903484609356, 12.45540543756422, 4.6f));
      cache.put("Scialla The Original Street Food", new Restaurant("Scialla The Original Street Food",
            "Pastas & traditional pizza pies served in an unassuming eatery with vegetarian options.",
            "Vicolo del Farinone, 27, 00193 Roma RM", 41.90369455835456, 12.459566517195528, 4.7f));
      cache.put("Trattoria Pizzeria Gli Archi", new Restaurant("Trattoria Pizzeria Gli Archi",
            "Traditional trattoria with exposed brick walls, serving up antipasti, pizzas & pasta dishes.",
            "Via Sebastiano Veniero, 26, 00192 Roma RM", 41.907930453801285, 12.455204785977637, 4.0f));
      cache.put("Alla Bracioleria Gracchi Restaurant", new Restaurant("Alla Bracioleria Gracchi Restaurant",
            "", "Via dei Gracchi, 19, 00192 Roma RM", 41.907129402661795, 12.458927251586584, 4.7f ));
      cache.put("Magazzino Scipioni", new Restaurant("Magazzino Scipioni",
            "Contemporary venue with a focus on unique wines & seasonal Italian plates, plus a bottle shop.",
            "Via degli Scipioni, 30, 00192 Roma RM", 41.90817843995448, 12.457118458698043, 4.6f));
      cache.put("Dal Toscano Restaurant", new Restaurant("Dal Toscano Restaurant",
            "Rich pastas, signature steaks & classic Tuscan dishes, plus Chianti wines, at a venerable trattoria.",
            "Via Germanico, 58-60, 00192 Roma RM", 41.90785274056548, 12.45822050287784, 4.2f));
      cache.put("Il Ciociaro", new Restaurant("Il Ciociaro",
            "Long-running, old-school restaurant plating traditional staples, from carbonara to tiramisu.",
            "Via Barletta, 21, 00192 Roma RM", 41.91038657525997, 12.458851939120656, 4.2f));

      String ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      Query<Restaurant> query = cache.query(ickle);
      query.setParameter("distance", 100);
      List<Restaurant> list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 150);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 150m) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 0.15 km) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, 0.0932057mi) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance yd) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 164.042);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance nmi) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 0.0809935);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni");

      ickle = String.format("from %s r " +
            "where r.location within circle(41.90847031512531, 12.455633288333539, :distance) ", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 250);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant");

      ickle = String.format("from %s r " +
            "where r.location within box(41.91, 12.45, 41.90, 12.46)", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Scialla The Original Street Food", "Alla Bracioleria Gracchi Restaurant");

      ickle = String.format("from %s r where r.location within" +
            " polygon((41.91, 12.45), (41.91, 12.46), (41.90, 12.46), (41.90, 12.46))", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactlyInAnyOrder("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Scialla The Original Street Food", "Alla Bracioleria Gracchi Restaurant");

      ickle = String.format("select distance(r.location, 41.90847031512531, 12.455633288333539) from %s r", RESTAURANT_ENTITY_NAME);
      Query<Object[]> projectQuery = cache.query(ickle);
      List<Object[]> projectList = projectQuery.list();
      assertThat(projectList).extracting(item -> item[0])
            .containsExactlyInAnyOrder(65.78997502576355, 622.8579549605669, 69.72458363789359, 310.6984480274634,
                  127.11531555461053, 224.8438726836208, 341.0897945700656);

      ickle = String.format("select distance(r.location, 41.90847031512531, 12.455633288333539, km) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList).extracting(item -> item[0])
            .containsExactlyInAnyOrder(0.06578997502576356, 0.6228579549605668, 0.06972458363789359, 0.31069844802746344,
                  0.12711531555461053, 0.2248438726836208, 0.34108979457006555);

      ickle = String.format("select distance(r.location, 41.90847031512531, 12.455633288333539), distance(r.location, 41.90847031512531, 12.455633288333539, km) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[0].equals(65.78997502576355)).extracting(item -> item[1]).first().isEqualTo(0.06578997502576356);
      assertThat(projectList)
            .filteredOn(item -> item[0].equals(622.8579549605669)).extracting(item -> item[1]).first().isEqualTo(0.6228579549605668);

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(65.78997502576355)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(622.8579549605669)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539, km) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.06578997502576356)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.6228579549605668)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539, mi) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.04087999521902312)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.3870259900683551)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539, yd) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(71.9487915854807)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(681.165742520305)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539, nm) from %s r", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
      projectList = projectQuery.list();
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.03552374461434317)).extracting(item -> item[0]).first().isEqualTo("La Locanda di Pietro");
      assertThat(projectList)
            .filteredOn(item -> item[1].equals(0.33631639036747674)).extracting(item -> item[0]).first().isEqualTo("Scialla The Original Street Food");

      ickle = String.format("from %s r order by distance(r.location, 41.90847031512531, 12.455633288333539)", RESTAURANT_ENTITY_NAME);
      query = cache.query(ickle);
      list = query.list();
      assertThat(list).extracting(Restaurant::name)
            .containsExactly("La Locanda di Pietro", "Trattoria Pizzeria Gli Archi", "Magazzino Scipioni",
                  "Dal Toscano Restaurant", "Alla Bracioleria Gracchi Restaurant", "Il Ciociaro",
                  "Scialla The Original Street Food");

      ickle = String.format("select r.name, distance(r.location, 41.90847031512531, 12.455633288333539) from %s r " +
            "order by distance(r.location, 41.90847031512531, 12.455633288333539)", RESTAURANT_ENTITY_NAME);
      projectQuery = cache.query(ickle);
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
   public void pointBindings() {
      cache.put(1, new Hiking("track 1", LatLng.of(41.907903484609356, 12.45540543756422),
            LatLng.of(41.90369455835456, 12.459566517195528)));
      cache.put(2, new Hiking("track 2", LatLng.of(41.90369455835456, 12.459566517195528),
            LatLng.of(41.907930453801285, 12.455204785977637)));
      cache.put(3, new Hiking("track 3", LatLng.of(41.907930453801285, 12.455204785977637),
            LatLng.of(41.907903484609356, 12.45540543756422)));

      String ickle = String.format("from %s r " +
            "where r.start within circle(41.90847031512531, 12.455633288333539, :distance) ", HIKING_ENTITY_NAME);
      Query<Hiking> query = cache.query(ickle);
      query.setParameter("distance", 150);
      List<Hiking> list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 1", "track 3");

      ickle = String.format("from %s r " +
            "where r.end within circle(41.90847031512531, 12.455633288333539, :distance) ", HIKING_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 150);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("from %s r " +
            "where r.end within box(:a, :b, :c, :d) ", HIKING_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("a", 42.00);
      query.setParameter("b", 12.00);
      query.setParameter("c", 41.00);
      query.setParameter("d", 12.459);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("from %s r " +
            "where r.end within polygon(:a, :b, :c, :d) ", HIKING_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("from %s r " +
            "where r.start within circle(41.90847031512531, 12.455633288333539, :distance) and  r.end within polygon(:a, :b, :c, :d) ", HIKING_ENTITY_NAME);
      query = cache.query(ickle);
      query.setParameter("distance", 150);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 3");
   }

   @Test
   public void indexingAndSearch_multiGeoPointEntities() {
      cache.put("Rome-Milan", new TrainRoute("Rome-Milan", ROME_COORDINATES.latitude(), ROME_COORDINATES.longitude(),
            MILAN_COORDINATES.latitude(), MILAN_COORDINATES.longitude()));
      cache.put("Bologna-Selva", new TrainRoute("Bologna-Selva", BOLOGNA_COORDINATES.latitude(), BOLOGNA_COORDINATES.longitude(),
            SELVA_COORDINATES.latitude(), SELVA_COORDINATES.longitude()));
      cache.put("Milan-Como", new TrainRoute("Milan-Como", MILAN_COORDINATES.latitude(), MILAN_COORDINATES.longitude(),
            COMO_COORDINATES.latitude(), COMO_COORDINATES.longitude()));
      cache.put("Bologna-Venice", new TrainRoute("Bologna-Venice", BOLOGNA_COORDINATES.latitude(), BOLOGNA_COORDINATES.longitude(),
            VENICE_COORDINATES.latitude(), VENICE_COORDINATES.longitude()));

      String ickle = String.format("from %s r where r.departure within circle(:lat, :lon, :distance)",
            TRAIN_ROUTE_ENTITY_NAME);
      Query<TrainRoute> trainQuery = cache.query(ickle);
      trainQuery.setParameter("lat", BOLOGNA_COORDINATES.latitude());
      trainQuery.setParameter("lon", BOLOGNA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 300_000);
      List<TrainRoute> trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Venice", "Bologna-Selva");

      ickle = String.format("from %s r where r.arrival within circle(:lat, :lon, :distance)",
            TRAIN_ROUTE_ENTITY_NAME);
      trainQuery = cache.query(ickle);
      trainQuery.setParameter("lat", SELVA_COORDINATES.latitude());
      trainQuery.setParameter("lon", SELVA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 200_000);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Bologna-Venice", "Bologna-Selva");

      ickle = String.format("from %s r where r.arrival within box(:a, :b, :c, :d)",
            TRAIN_ROUTE_ENTITY_NAME);
      trainQuery = cache.query(ickle);
      trainQuery.setParameter("a", 47.00);
      trainQuery.setParameter("b", 8.00);
      trainQuery.setParameter("c", 45.70);
      trainQuery.setParameter("d", 12.00);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Selva");

      ickle = String.format("from %s r where r.arrival within polygon(:a, :b, :c, :d)",
            TRAIN_ROUTE_ENTITY_NAME);
      trainQuery = cache.query(ickle);
      trainQuery.setParameter("a", "(47.00, 8.00)");
      trainQuery.setParameter("b", "(47.00, 12.00)");
      trainQuery.setParameter("c", "(45.70, 12.00)");
      trainQuery.setParameter("d", "(45.70, 8.00)");
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como", "Bologna-Selva");

      ickle = String.format("from %s r where r.departure within circle(:lat, :lon, :distance) and r.arrival within circle(:lat1, :lon1, :distance1)",
            TRAIN_ROUTE_ENTITY_NAME);
      trainQuery = cache.query(ickle);
      trainQuery.setParameter("lat", BOLOGNA_COORDINATES.latitude());
      trainQuery.setParameter("lon", BOLOGNA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 300_000);
      trainQuery.setParameter("lat1", SELVA_COORDINATES.latitude());
      trainQuery.setParameter("lon1", SELVA_COORDINATES.longitude());
      trainQuery.setParameter("distance1", 200_000);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Bologna-Venice", "Bologna-Selva");

      ickle = String.format("from %s r where r.departure within circle(:lat, :lon, :distance) and r.arrival not within circle(:lat1, :lon1, :distance1)",
            TRAIN_ROUTE_ENTITY_NAME);
      trainQuery = cache.query(ickle);
      trainQuery.setParameter("lat", BOLOGNA_COORDINATES.latitude());
      trainQuery.setParameter("lon", BOLOGNA_COORDINATES.longitude());
      trainQuery.setParameter("distance", 300_000);
      trainQuery.setParameter("lat1", SELVA_COORDINATES.latitude());
      trainQuery.setParameter("lon1", SELVA_COORDINATES.longitude());
      trainQuery.setParameter("distance1", 200_000);
      trainRoutes = trainQuery.list();
      assertThat(trainRoutes).extracting(TrainRoute::name)
            .containsExactlyInAnyOrder("Milan-Como");
   }
}
