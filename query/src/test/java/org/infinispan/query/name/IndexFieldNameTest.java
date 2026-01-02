package org.infinispan.query.name;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.query.model.ChangeName;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.name.IndexFieldNameTest")
public class IndexFieldNameTest extends SingleCacheManagerTest {

   public static final String ENTITY_NAME = ChangeName.class.getName();

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.statistics().enable();
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(ChangeName.class);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Test
   public void test() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);
      Map<String, IndexMetamodel> metamodel = searchMapping.metamodel();
      assertThat(metamodel).containsKeys(ENTITY_NAME);
      assertThat(metamodel.get(ENTITY_NAME).getValueFields()).containsKeys(ChangeName.INDEX_FIELD_NAME);

      QueryStatistics statistics = Search.getSearchStatistics(cache).getQueryStatistics();
      statistics.clear();

      cache.put("one", new ChangeName("one"));
      cache.put("two", new ChangeName("two"));
      cache.put("three", new ChangeName("three"));

      Query<ChangeName> query = cache.query(String.format("from %s c where c.%s = 'two'", ENTITY_NAME, ChangeName.DATA_FIELD_NAME));
      List<ChangeName> list = query.list();
      assertThat(list).extracting(ChangeName.DATA_FIELD_NAME).containsExactly("two");

      assertThat(statistics.getNonIndexedQueryCount()).isOne();
      assertThat(statistics.getLocalIndexedQueryCount()).isZero();

      query = cache.query(String.format("from %s c where c.%s = 'two'", ENTITY_NAME, ChangeName.INDEX_FIELD_NAME));
      list = query.list();
      assertThat(list).extracting(ChangeName.DATA_FIELD_NAME).containsExactly("two");

      assertThat(statistics.getNonIndexedQueryCount()).isOne();
      assertThat(statistics.getLocalIndexedQueryCount()).isOne();
   }
}
