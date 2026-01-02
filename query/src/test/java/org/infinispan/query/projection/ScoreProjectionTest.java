package org.infinispan.query.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.hibernate.search.engine.search.predicate.SearchPredicate;
import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.query.SearchResult;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.scope.SearchScope;
import org.infinispan.query.mapper.session.SearchSession;
import org.infinispan.query.model.Game;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.projection.ScoreProjectionTest")
public class ScoreProjectionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Game.class);

      return TestCacheManagerFactory.createCacheManager(indexed);
   }

   @BeforeMethod
   public void setUp() {
      if (!cache.isEmpty()) {
         return;
      }

      cache.put(4, new Game("4", "descriAAion"));
      cache.put(1, new Game("1", "description"));
      cache.put(3, new Game("3", "desription"));
      cache.put(5, new Game("5", "very-different"));
      cache.put(2, new Game("2", "descriqtion"));
   }

   @Test
   public void entityNoScore() {
      Query<Object[]> query;
      List<Object[]> games;

      // order by field to not enforce score!
      query = cache.query("select g from org.infinispan.query.model.Game g where g.description : 'description'~2 order by g.name desc");
      games = query.list();
      assertThat(games).extracting(objects -> objects[0]).extracting("name").containsExactly("4", "3", "2", "1");
   }

   @Test
   public void entityAndScore() {
      Query<Object[]> query;
      List<Object[]> games;

      query = cache.query("select g, score(g) from org.infinispan.query.model.Game g where g.description : 'description'~2");
      games = query.list();
      assertThat(games).extracting(objects -> objects[0]).extracting("name").containsExactly("1", "2", "3", "4");
      assertThat(games).extracting(objects -> objects[1]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
   }

   @Test
   public void fieldAndScore() {
      Query<Object[]> query;
      List<Object[]> games;

      query = cache.query("select score(g), g.name from org.infinispan.query.model.Game g where g.description : 'description'~2");
      games = query.list();
      assertThat(games).extracting(objects -> objects[1]).containsExactly("1", "2", "3", "4");
      assertThat(games).extracting(objects -> objects[0]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
   }

   @Test
   public void entityScoreAndField() {
      Query<Object[]> query;
      List<Object[]> games;

      query = cache.query("select g.name, score(g), g from org.infinispan.query.model.Game g where g.description : 'description'~2");
      games = query.list();
      assertThat(games).extracting(objects -> objects[0]).containsExactly("1", "2", "3", "4");
      assertThat(games).extracting(objects -> objects[1]).hasOnlyElementsOfType(Float.class).isNotNull().allMatch(o -> !o.equals(Float.NaN));
      assertThat(games).extracting(objects -> objects[2]).extracting("name").containsExactly("1", "2", "3", "4");
   }

   @Test
   public void nativeQuery() {
      ComponentRegistry componentRegistry = ComponentRegistry.of(cache);
      SearchMapping searchMapping = componentRegistry.getComponent(SearchMapping.class);
      assertThat(searchMapping).isNotNull();

      SearchSession mappingSession = searchMapping.getMappingSession();
      SearchScope<Game> scope = mappingSession.scope(Game.class);

      SearchProjection<List<?>> projection = scope.projection().composite(scope.projection().score(), scope.projection().entity()).toProjection();
      SearchPredicate predicate = scope.predicate().match().field("description").matching("description").fuzzy(2).toPredicate();

      SearchQuery<List<?>> nativeQuery = mappingSession.search(scope)
            .select(projection)
            .where(predicate)
            .toQuery();
      SearchResult<List<?>> result = nativeQuery.fetch(100);
      assertThat(result).isNotNull();
   }
}
