package org.infinispan.query.pagination.pagination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Developer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.pagination.PaginationTest")
@TestForIssue(jiraKey = "ISPN-16585")
public class PaginationTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Developer.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   protected void createBeforeMethod() {
      before(cache);
   }

   @Test
   public void hybrid() {
      hybrids(cache);
      hybrids_max3(cache);
   }

   @Test
   public void entityProjection() {
      entityProjection(cache);
      entityProjection_max3(cache);
   }

   @Test
   public void defaultProjection() {
      defaultProjection(cache);
      defaultProjection_max3(cache);
   }

   private static Developer getDev() {
      return new Developer("alukard", "alukard@rage.io", "Infinispan developer", 2000, "Infinispan developer");
   }

   static void before(Cache<Object, Object> cache) {
      if (cache.isEmpty()) {
         for (int i = 0; i < 10; i++) {
            cache.put(i, getDev());
         }
      }
   }

   static void hybrids(Cache<Object, Object> cache) {
      Query<Object[]> query = cache.query(String.format(
            "select d.nonIndexed, d from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      QueryResult<Object[]> result = query.execute();
      assertThat(result.list().size()).isEqualTo(10);
      assertThat(result.count().value()).isEqualTo(10);
   }

   static void hybrids_max3(Cache<Object, Object> cache) {
      Query<Object[]> query = cache.query(String.format(
            "select d.nonIndexed, d from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      query.maxResults(3);
      QueryResult<Object[]> result = query.execute();
      Developer dev = getDev();
      String nonIndexed = dev.getNonIndexed();
      assertThat(result.list()).extracting(item -> item[0]).containsExactly(nonIndexed, nonIndexed, nonIndexed);
      assertThat(result.list()).extracting(item -> item[1]).containsExactly(dev, dev, dev);
      assertThat(result.count().value()).isEqualTo(10);
   }

   static void entityProjection(Cache<Object, Object> cache) {
      Query<Object[]> query = cache.query(String.format(
            "select d from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      QueryResult<Object[]> result = query.execute();
      assertThat(result.list().size()).isEqualTo(10);
      assertThat(result.count().value()).isEqualTo(10);
   }

   static void entityProjection_max3(Cache<Object, Object> cache) {
      Query<Object[]> query = cache.query(String.format(
            "select d from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      query.maxResults(3);
      Developer dev = getDev();
      QueryResult<Object[]> result = query.execute();
      assertThat(result.list()).extracting(item -> item[0]).containsExactly(dev, dev, dev);
      assertThat(result.count().value()).isEqualTo(10);
   }

   static void defaultProjection(Cache<Object, Object> cache) {
      Query<Developer> query = cache.query(String.format(
            "from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      QueryResult<Developer> result = query.execute();
      assertThat(result.list().size()).isEqualTo(10);
      assertThat(result.count().value()).isEqualTo(10);
   }

   static void defaultProjection_max3(Cache<Object, Object> cache) {
      Query<Developer> query = cache.query(String.format(
            "from %s d where d.biography : 'Infinispan'",
            Developer.class.getName()));
      query.maxResults(3);
      QueryResult<Developer> result = query.execute();
      Developer dev = getDev();
      assertThat(result.list()).containsExactly(dev, dev, dev);
      assertThat(result.count().value()).isEqualTo(10);
   }
}
