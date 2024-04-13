package org.infinispan.client.hotrod.query.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Structure;
import org.infinispan.client.hotrod.annotation.model.StructureKey;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.model.Item;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.key.PojoKeyEntriesRemoteQueryTest")
public class PojoKeyEntriesRemoteQueryTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.statistics().enable();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("model.Structure");

      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Structure.StructureSchema.INSTANCE;
   }

   @BeforeMethod
   public void setUp() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      if (!remoteCache.isEmpty()) {
         return;
      }

      for (byte item = 1; item <= 10; item++) {
         StructureKey key = new StructureKey("z" + item, (int) item, item * item);
         remoteCache.put(key, new Structure("c" + item, "bla bla bla", (int)item, key));
      }
   }

   @Test
   public void test_embedded() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Item> query = remoteCache.query("from model.Structure s where s.nested.zone = 'z7'");
      List<Item> list = query.list();
      assertThat(list).extracting("code").containsExactly("c7");

      query = remoteCache.query("from model.Structure s where s.nested.column = 9");
      list = query.list();
      assertThat(list).extracting("code").containsExactly("c3");

      expectedIndexedQueries(2);
   }

   @Test
   public void test_key() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Item> query = remoteCache.query("from model.Structure s where s.key.zone = 'z7'");
      List<Item> list = query.list();
      assertThat(list).extracting("code").containsExactly("c7");

      query = remoteCache.query("from model.Structure s where s.key.column = 9");
      list = query.list();
      assertThat(list).extracting("code").containsExactly("c3");

      expectedIndexedQueries(2);
   }

   @Test
   public void testProjections_keyEmbCombinations() {
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      Query<Object[]> query = remoteCache.query("select s.nested.column from model.Structure s where s.nested.zone = 'z7'");
      List<Object[]> list = query.list();
      assertThat(list).extracting(objects -> objects[0]).containsExactly(49);

      remoteCache = remoteCacheManager.getCache();
      query = remoteCache.query("select s.key.column from model.Structure s where s.nested.zone = 'z7'");
      list = query.list();
      assertThat(list).extracting(objects -> objects[0]).containsExactly(49);

      remoteCache = remoteCacheManager.getCache();
      query = remoteCache.query("select s.nested.column from model.Structure s where s.key.zone = 'z7'");
      list = query.list();
      assertThat(list).extracting(objects -> objects[0]).containsExactly(49);

      remoteCache = remoteCacheManager.getCache();
      query = remoteCache.query("select s.key.column from model.Structure s where s.key.zone = 'z7'");
      list = query.list();
      assertThat(list).extracting(objects -> objects[0]).containsExactly(49);

      expectedIndexedQueries(4);
   }

   private void expectedIndexedQueries(int expectedIndexedQueries) {
      QueryStatistics queryStatistics = Search.getSearchStatistics(cache).getQueryStatistics();
      assertThat(queryStatistics.getLocalIndexedQueryCount()).isEqualTo(expectedIndexedQueries);
      assertThat(queryStatistics.getHybridQueryCount()).isZero();
      assertThat(queryStatistics.getNonIndexedQueryCount()).isZero();
      queryStatistics.clear();
   }
}
