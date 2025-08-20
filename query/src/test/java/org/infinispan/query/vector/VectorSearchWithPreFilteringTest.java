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
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Item;
import org.infinispan.query.model.Metadata;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.scope.SearchScope;
import org.infinispan.query.mapper.session.SearchSession;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.vector.VectorSearchWithPreFilteringTest")
public class VectorSearchWithPreFilteringTest extends SingleCacheManagerTest {

   private static final String[] BUGGY_OPTIONS =
         {"cat lover", "code lover", "mystical", "philologist", "algorithm designer", "decisionist", "philosopher"};

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Item.class);
      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @BeforeMethod
   public void beforeClass() {
      for (byte item = 1; item <= 50; item++) {
         byte[] bytes = {item, item, item};
         String buggy = BUGGY_OPTIONS[item % 7];
         cache.put(item, new Item("c" + item, bytes, new float[]{1.1f * item, 1.1f * item, 1.1f * item}, buggy, (int) item, List.of(new Metadata("animal", "cat"))));
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
            .extracting("entity").extracting("code").containsExactly("c7", "c8", "c1");
   }

   @Test
   public void ickleQuery_simpleFiltering() {
      Query<Object[]> query = cache.query(
            "select score(i), i from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c14", "c21");
   }

   @Test
   public void ickleQuery_complexFiltering() {
      Query<Object[]> query = cache.query(
            "select score(i), i from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Object[]> hits = query.list();
      assertThat(hits).extracting(objects -> objects[1])
            .extracting("code").containsExactly("c7", "c8", "c1");
   }

   @Test
   public void entityProjection() {
      Query<Item> query = cache.query(
            "from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering i.buggy : 'cat'");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      List<Item> hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c14", "c21");

      query = cache.query(
            "from org.infinispan.query.model.Item i where i.floatVector <-> [:a]~:k filtering (i.buggy : 'cat' or i.buggy : 'code')");
      query.setParameter("a", new float[]{7.0f, 7.0f, 7.0f});
      query.setParameter("k", 3);

      hits = query.list();
      assertThat(hits).extracting("code").containsExactly("c7", "c8", "c1");
   }
}
