package org.infinispan.query.backend;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests to verify if unnecessary index operations are sent.
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.QueryInterceptorIndexingOperationsTest")
public class QueryInterceptorIndexingOperationsTest extends SingleCacheManagerTest {

   public QueryInterceptorIndexingOperationsTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Test
   public void testAvoidUnnecessaryRemoveForSimpleUpdate() throws IOException {
      Directory directory = initializeAndExtractDirectory(cache, Entity1.class);

      Entity1 entity1 = new Entity1("e1");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, cache, () -> cache.put(1, new Entity1("e2")));

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideNonIndexedByIndexed() throws Exception {
      Directory directory = initializeAndExtractDirectory(cache, Entity1.class);

      cache.put(1, "string value");

      long commits = doRecordingCommits(directory, cache, () -> cache.put(1, new Entity1("e1")) );

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideIndexedByNonIndexed() throws IOException {
      Directory directory = initializeAndExtractDirectory(cache, Entity1.class);

      final Entity1 entity1 = new Entity1("title");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, cache, () -> cache.put(1, "another"));

      assertEquals(1, commits);
      assertEquals(0, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideIndexedByOtherIndexed() throws IOException {
      Directory directory1 = initializeAndExtractDirectory(cache, Entity1.class);
      Directory directory2 = initializeAndExtractDirectory(cache, Entity2.class);

      final Entity1 entity1 = new Entity1("title");
      cache.put(1, entity1);

      long initialGenDir1 = SegmentInfos.getLastCommitGeneration(directory1);
      long commitsDir2 = doRecordingCommits(directory2, cache, () -> cache.put(1, new Entity2("title2")));
      long commitsDir1 = SegmentInfos.getLastCommitGeneration(directory1) - initialGenDir1;

      assertEquals(3, commitsDir1 + commitsDir2);
      assertEquals(0, countIndexedDocuments(Entity1.class));
      assertEquals(1, countIndexedDocuments(Entity2.class));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable()
            .addIndexedEntity(Entity1.class)
            .addIndexedEntity(Entity2.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.COMMIT_INTERVAL, "100000000");

      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder()
            .clusteredDefault()
            .defaultCacheName(TestCacheManagerFactory.DEFAULT_CACHE_NAME)
            .serialization().addContextInitializer(QueryTestSCI.INSTANCE);

      holder.getNamedConfigurationBuilders().put(TestCacheManagerFactory.DEFAULT_CACHE_NAME, builder);
      return TestCacheManagerFactory.newDefaultCacheManager(true, holder);
   }

   interface Operation {
      void execute();
   }

   static long doRecordingCommits(Directory directory, Cache<Object, Object> cache, Operation operation) throws IOException {
      long initialGen = SegmentInfos.getLastCommitGeneration(directory);

      // if the file is not already present gen is -1,
      // after the first change it will become 1
      if (initialGen == -1) {
         initialGen = 0;
      }

      operation.execute();
      TestQueryHelperFactory.extractSearchMapping(cache).scopeAll().workspace().flush();
      return SegmentInfos.getLastCommitGeneration(directory) - initialGen;
   }

   private Directory initializeAndExtractDirectory(Cache cache, Class<?> entityType) {
      return IndexAccessor.of(cache, entityType).getDirectory();
   }

   private int countIndexedDocuments(Class<?> clazz) {
      Query query = Search.getQueryFactory(cache).create("FROM " + clazz.getName());
      return query.list().size();
   }

   @Indexed(index = "theIndex1")
   @SuppressWarnings("unused")
   static class Entity1 {

      @Field
      private final String attribute;

      public Entity1(String attribute) {
         this.attribute = attribute;
      }
   }

   @Indexed(index = "theIndex2")
   @SuppressWarnings("unused")
   static class Entity2 {

      @Field
      private final String attribute;

      public Entity2(String attribute) {
         this.attribute = attribute;
      }
   }
}
