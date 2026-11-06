package org.infinispan.client.hotrod.query.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.Hiking;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.geo.RemoteGeoPointQueryTest")
public class RemoteGeoPointQueryTest extends SingleHotRodServerTest {

   public static final String HIKING_ENTITY_NAME = "geo.Hiking";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(HIKING_ENTITY_NAME);
      return TestCacheManagerFactory.createServerModeCacheManager(contextInitializer(), config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Hiking.HikingSchema.INSTANCE;
   }

   @Test
   public void verifySpatialMapping() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();
      assertThat(metamodel).containsKeys(HIKING_ENTITY_NAME);
      IndexMetamodel indexMetamodel = metamodel.get(HIKING_ENTITY_NAME);
      assertThat(indexMetamodel.getValueFields().keySet())
            .containsExactlyInAnyOrder("name", "start", "end");
   }

   @Test
   public void indexingAndSearch() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      remoteCache.putAll(Hiking.data());

      String ickle = String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance)""", HIKING_ENTITY_NAME);
      Query<Hiking> query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      List<Hiking> list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 1", "track 3");

      ickle = String.format("""
            from %s r
            where r.end within circle(41.90847031512531, 12.455633288333539, :distance)""", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance) and r.end not within circle(41.90847031512531, 12.455633288333539, :distance1)""", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      query.setParameter("distance1", 150);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 1");

      ickle = String.format("""
            from %s r
            where r.end within box(:a, :b, :c, :d)""", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("a", 42.00);
      query.setParameter("b", 12.00);
      query.setParameter("c", 41.00);
      query.setParameter("d", 12.459);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("""
            from %s r
            where r.end within polygon(:a, :b, :c, :d)""", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("""
            select r.name, distance(r.end, 41.91, 12.46)
            from %s r
            where r.start within polygon(:a, :b, :c, :d)
            order by distance(r.end, 41.91, 12.46) desc""", HIKING_ENTITY_NAME);
      Query<Object[]> proQuery = remoteCache.query(ickle);
      proQuery.setParameter("a", "(42.00, 12.00)");
      proQuery.setParameter("b", "(42.00, 12.459)");
      proQuery.setParameter("c", "(41.00, 12.459)");
      proQuery.setParameter("d", "(41.00, 12.00)");
      QueryResult<Object[]> result = proQuery.execute();
      assertThat(result.count().value()).isEqualTo(2);
      List<Object[]> proList = result.list();
      assertThat(proList).extracting(item -> item[0]).containsExactly("track 1", "track 3");
      assertThat(proList).extracting(item -> item[1]).containsExactly(702.0532157425224, 445.9892727223779);

      ickle = String.format("""
            select r.name, distance(r.start, 41.91, 12.46)
            from %s r
            where r.end within polygon(:a, :b, :c, :d)
            order by distance(r.start, 41.91, 12.46) desc""", HIKING_ENTITY_NAME);
      proQuery = remoteCache.query(ickle);
      proQuery.setParameter("a", "(42.00, 12.00)");
      proQuery.setParameter("b", "(42.00, 12.459)");
      proQuery.setParameter("c", "(41.00, 12.459)");
      proQuery.setParameter("d", "(41.00, 12.00)");
      result = proQuery.execute();
      assertThat(result.count().value()).isEqualTo(2);
      proList = result.list();
      assertThat(proList).extracting(item -> item[0]).containsExactly("track 2", "track 3");
      assertThat(proList).extracting(item -> item[1]).containsExactly(702.0532157425224, 458.7166803703988);
   }

   @Test
   public void mixedSpatialPredicates() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      remoteCache.putAll(Hiking.data());

      // circle(start) returns [track 1, track 3]
      // polygon(end) returns [track 2, track 3]
      // intersection should be [track 3]
      Query<Hiking> query = remoteCache.query(String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance)
            and r.end within polygon(:a, :b, :c, :d)""", HIKING_ENTITY_NAME));
      query.setParameter("distance", 150);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      List<Hiking> list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 3");

      // circle(start) AND box(end)
      query = remoteCache.query(String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance)
            and r.end within box(:a, :b, :c, :d)""", HIKING_ENTITY_NAME));
      query.setParameter("distance", 150);
      query.setParameter("a", 42.00);
      query.setParameter("b", 12.00);
      query.setParameter("c", 41.00);
      query.setParameter("d", 12.459);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 3");

      // NOT polygon(end)
      query = remoteCache.query(String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance)
            and r.end not within polygon(:a, :b, :c, :d)""", HIKING_ENTITY_NAME));
      query.setParameter("distance", 150);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 1");

      // NOT box(end)
      query = remoteCache.query(String.format("""
            from %s r
            where r.start within circle(41.90847031512531, 12.455633288333539, :distance)
            and r.end not within box(:a, :b, :c, :d)""", HIKING_ENTITY_NAME));
      query.setParameter("distance", 150);
      query.setParameter("a", 42.00);
      query.setParameter("b", 12.00);
      query.setParameter("c", 41.00);
      query.setParameter("d", 12.459);
      list = query.list();
      assertThat(list).extracting(Hiking::name)
            .containsExactlyInAnyOrder("track 1");
   }
}
