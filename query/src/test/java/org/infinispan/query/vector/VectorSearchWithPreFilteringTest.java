package org.infinispan.query.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.predicate.dsl.MatchPredicateOptionsStep;
import org.hibernate.search.engine.search.predicate.dsl.SimpleBooleanPredicateOptionsStep;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.query.SearchResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Item;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.vector.VectorSearchWithPreFilteringTest")
public class VectorSearchWithPreFilteringTest extends SingleCacheManagerTest {

   private static final String[] BUGGY_OPTIONS =
         {"cat lover", "code lover", "mystical", "philologist", "algorithm designer", "decisionist", "philosopher"};

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Item.class);
      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @BeforeClass
   public void beforeClass() {
      for (byte item = 1; item <= 50; item++) {
         byte[] bytes = {item, item, item};
         String buggy = BUGGY_OPTIONS[item % 7];
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, buggy, (int) item));
      }
   }

   @Test
   public void nativeQuery() {
      SearchSession mappingSession = ComponentRegistry.of(cache).getComponent(SearchMapping.class).getMappingSession();
      SearchScope<Item> scope = mappingSession.scope(Item.class);

      MatchPredicateOptionsStep<?> catMatching = scope.predicate().match().field("buggy").matching("cat");
      MatchPredicateOptionsStep<?> codeMatching = scope.predicate().match().field("buggy").matching("code");
      SimpleBooleanPredicateOptionsStep<?> preFilteringMatching = scope.predicate().or(catMatching, codeMatching);
      SearchPredicate predicate = scope.predicate().knn(3).field("floatVector").matching(7, 7, 7)
            .filter(preFilteringMatching).toPredicate();

      SearchProjection<List<?>> projection = scope.projection().composite(
            scope.projection().score(), scope.projection().entity()).toProjection();

      SearchQuery<List<?>> nativeQuery = mappingSession.search(scope)
            .select(projection)
            .where(predicate)
            .toQuery();
      SearchResult<List<?>> result = nativeQuery.fetch(100);
      assertThat(result.hits()).extracting(objects -> objects.get(1))
            .extracting("code").containsExactly("c7", "c8", "c1");
   }
}
