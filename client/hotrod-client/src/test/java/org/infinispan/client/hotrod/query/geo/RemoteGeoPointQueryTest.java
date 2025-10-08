package org.infinispan.client.hotrod.query.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.query.testdomain.protobuf.ProtoHiking;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.api.query.geo.LatLng;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.geo.RemoteGeoPointQueryTest")
public class RemoteGeoPointQueryTest extends SingleHotRodServerTest {

   public static final String HIKING_ENTITY_NAME = "geo.ProtoHiking";

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
      return ProtoHiking.ProtoHikingSchema.INSTANCE;
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

      remoteCache.put(1, new ProtoHiking("track 1", LatLng.of(41.907903484609356, 12.45540543756422),
            LatLng.of(41.90369455835456, 12.459566517195528)));
      remoteCache.put(2, new ProtoHiking("track 2", LatLng.of(41.90369455835456, 12.459566517195528),
            LatLng.of(41.907930453801285, 12.455204785977637)));
      remoteCache.put(3, new ProtoHiking("track 3", LatLng.of(41.907930453801285, 12.455204785977637),
            LatLng.of(41.907903484609356, 12.45540543756422)));

      String ickle = String.format("from %s r " +
            "where r.start within circle(41.90847031512531, 12.455633288333539, :distance) ", HIKING_ENTITY_NAME);
      Query<ProtoHiking> query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      List<ProtoHiking> list = query.list();
      assertThat(list).extracting(ProtoHiking::name)
            .containsExactlyInAnyOrder("track 1", "track 3");

      ickle = String.format("from %s r " +
            "where r.end within circle(41.90847031512531, 12.455633288333539, :distance) ", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      list = query.list();
      assertThat(list).extracting(ProtoHiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("from %s r " +
            "where r.start within circle(41.90847031512531, 12.455633288333539, :distance) and r.end not within circle(41.90847031512531, 12.455633288333539, :distance1) ", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("distance", 150);
      query.setParameter("distance1", 150);
      list = query.list();
      assertThat(list).extracting(ProtoHiking::name)
            .containsExactlyInAnyOrder("track 1");

      ickle = String.format("from %s r " +
            "where r.end within box(:a, :b, :c, :d) ", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("a", 42.00);
      query.setParameter("b", 12.00);
      query.setParameter("c", 41.00);
      query.setParameter("d", 12.459);
      list = query.list();
      assertThat(list).extracting(ProtoHiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      ickle = String.format("from %s r " +
            "where r.end within polygon(:a, :b, :c, :d) ", HIKING_ENTITY_NAME);
      query = remoteCache.query(ickle);
      query.setParameter("a", "(42.00, 12.00)");
      query.setParameter("b", "(42.00, 12.459)");
      query.setParameter("c", "(41.00, 12.459)");
      query.setParameter("d", "(41.00, 12.00)");
      list = query.list();
      assertThat(list).extracting(ProtoHiking::name)
            .containsExactlyInAnyOrder("track 2", "track 3");

      Query<Object[]> proQuery = remoteCache.query(
            "select r.name, distance(r.end, 41.91, 12.46) " +
            "from geo.ProtoHiking r " +
            "where r.start within polygon(:a, :b, :c, :d) " +
            "order by distance(r.end, 41.91, 12.46) desc");
      proQuery.setParameter("a", "(42.00, 12.00)");
      proQuery.setParameter("b", "(42.00, 12.459)");
      proQuery.setParameter("c", "(41.00, 12.459)");
      proQuery.setParameter("d", "(41.00, 12.00)");
      QueryResult<Object[]> result = proQuery.execute();
      assertThat(result.count().value()).isEqualTo(2);
      List<Object[]> proList = result.list();
      assertThat(proList).extracting(item -> item[0]).containsExactly("track 1", "track 3");
      assertThat(proList).extracting(item -> item[1]).containsExactly(702.0532157425224, 445.9892727223779);

      proQuery = remoteCache.query(
            "select r.name, distance(r.start, 41.91, 12.46) " +
                  "from geo.ProtoHiking r " +
                  "where r.end within polygon(:a, :b, :c, :d) " +
                  "order by distance(r.start, 41.91, 12.46) desc");
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
}
