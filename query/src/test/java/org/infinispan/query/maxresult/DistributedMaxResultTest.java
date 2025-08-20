package org.infinispan.query.maxresult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Developer;
import org.infinispan.query.model.Game;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.maxresult.DistributedMaxResultTest")
public class DistributedMaxResultTest extends MultipleCacheManagersTest {

   private Cache<Integer, Game> node1;
   private Cache<Integer, Developer> node2;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      config
            .clustering()
               .hash().numOwners(2)
               .stateTransfer().chunkSize(100)
            .indexing().enable()
               .storage(LOCAL_HEAP)
               .addIndexedEntity("org.infinispan.query.model.Game")
               .addIndexedEntity(Developer.class)
            .query().defaultMaxResults(50);

      createClusteredCaches(2, config);
      node1 = cache(0);
      node2 = cache(1);
   }

   @Test
   public void executeSmokeTest() {
      for (int i = 1; i <= 110; i++) {
         node1.put(i, new Game("Game " + i, "This is the game " + i + "# of a series"));
      }

      Query<Game> query = node1.query("from org.infinispan.query.model.Game");
      QueryResult<Game> result = query.execute();

      assertThat(result.count().value()).isEqualTo(110);
      assertThat(result.list()).hasSize(50); // use custom default

      query = node1.query("from org.infinispan.query.model.Game");
      query.maxResults(200); // raise it
      result = query.execute();

      assertThat(result.count().value()).isEqualTo(110);
      assertThat(result.list()).hasSize(110);
   }

   @Test
   @TestForIssue(jiraKey = "ISPN-16808")
   public void deleteMoreThan100Elements() {
      for (int i=0; i<60; i++) {
         Developer developer = new Developer("james", "james@blablabla.edu", "Hibernate developer", 2004, "Hibernate developer");
         node2.put(i, developer);
      }

      Query<Object[]> query = node2.query("select count(d) from org.infinispan.query.model.Developer d");
      List<Object[]> result = query.list();
      assertThat(result).extracting(item -> item[0]).containsExactly(60L);

      node2.query("delete from org.infinispan.query.model.Developer").execute();

      query = node2.query("select count(d) from org.infinispan.query.model.Developer d");
      result = query.list();
      assertThat(result).extracting(item -> item[0]).containsExactly(0L);
   }
}
