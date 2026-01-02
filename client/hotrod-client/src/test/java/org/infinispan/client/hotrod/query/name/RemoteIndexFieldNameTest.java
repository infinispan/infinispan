package org.infinispan.client.hotrod.query.name;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.query.model.ChangeName;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.name.RemoteIndexFieldNameTest")
public class RemoteIndexFieldNameTest extends SingleHotRodServerTest {

   public static final String ENTITY_NAME = "bla.ChangeName";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(ENTITY_NAME);
      return TestCacheManagerFactory.createServerModeCacheManager(config);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return ChangeName.ChangeNameSchema.INSTANCE;
   }

   @Test
   public void test() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();
      assertThat(metamodel).containsKeys(ENTITY_NAME);
      assertThat(metamodel.get(ENTITY_NAME).getValueFields()).containsKeys(ChangeName.INDEX_FIELD_NAME);

      QueryStatistics statistics = Search.getSearchStatistics(cache).getQueryStatistics();
      statistics.clear();

      RemoteCache<String, ChangeName> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("one", new ChangeName("one"));
      remoteCache.put("two", new ChangeName("two"));
      remoteCache.put("three", new ChangeName("three"));

      Query<ChangeName> query = remoteCache.query(String.format("from %s c where c.%s = 'two'", ENTITY_NAME, ChangeName.DATA_FIELD_NAME));
      List<ChangeName> list = query.list();
      assertThat(list).extracting(ChangeName.DATA_FIELD_NAME).containsExactly("two");

      assertThat(statistics.getNonIndexedQueryCount()).isOne();
      assertThat(statistics.getLocalIndexedQueryCount()).isZero();

      query = remoteCache.query(String.format("from %s c where c.%s = 'two'", ENTITY_NAME, ChangeName.INDEX_FIELD_NAME));
      list = query.list();
      assertThat(list).extracting(ChangeName.DATA_FIELD_NAME).containsExactly("two");

      assertThat(statistics.getNonIndexedQueryCount()).isOne();
      assertThat(statistics.getLocalIndexedQueryCount()).isOne();
   }
}
