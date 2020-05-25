package org.infinispan.query.backend;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.impl.ComponentRegistryUtils;
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

   public void testAvoidUnnecessaryRemoveForSimpleUpdate() throws Exception {
      Directory directory = initializeAndExtractDirectory(cache);

      Entity1 entity1 = new Entity1("e1");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, () -> cache.put(1, new Entity1("e2")));

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   public void testOverrideNonIndexedByIndexed() throws Exception {
      Directory directory = initializeAndExtractDirectory(cache);

      cache.put(1, "string value");

      long commits = doRecordingCommits(directory, () -> cache.put(1, new Entity1("e1")));

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   public void testOverrideIndexedByNonIndexed() throws Exception {
      Directory directory = initializeAndExtractDirectory(cache);

      final Entity1 entity1 = new Entity1("title");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, () -> cache.put(1, "another"));

      assertEquals(1, commits);
      assertEquals(0, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   public void testOverrideIndexedByOtherIndexed() throws Exception {
      Directory directory = initializeAndExtractDirectory(cache);

      final Entity1 entity1 = new Entity1("title");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, () -> cache.put(1, new Entity2("title2")));

      assertEquals(2, commits);
      assertEquals(0, countIndexedDocuments(Entity1.class));
      assertEquals(1, countIndexedDocuments(Entity2.class));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.indexing().enable()
            .addIndexedEntity(Entity1.class)
            .addIndexedEntity(Entity2.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

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

   static long doRecordingCommits(Directory directory, Operation operation) throws IOException {
      long initialGen = SegmentInfos.getLastCommitGeneration(directory);
      operation.execute();
      return SegmentInfos.getLastCommitGeneration(directory) - initialGen;
   }

   private Directory initializeAndExtractDirectory(Cache<?, ?> cache) {
      SearchIntegrator searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
      DirectoryBasedIndexManager indexManager = (DirectoryBasedIndexManager) searchFactory.getIndexBindings().get(PojoIndexedTypeIdentifier.convertFromLegacy(Entity1.class))
            .getIndexManagerSelector().all().iterator().next();
      return indexManager.getDirectoryProvider().getDirectory();
   }

   private long countIndexedDocuments(Class<?> clazz) {
      Query<?> query = Search.getQueryFactory(cache).create("FROM " + clazz.getName());
      return query.execute().hitCount().orElse(-1);
   }

   @Indexed(index = "theIndex")
   static class Entity1 {

      @Field
      private final String attribute;

      public Entity1(String attribute) {
         this.attribute = attribute;
      }

      public String getAttribute() {
         return attribute;
      }
   }

   @Indexed(index = "theIndex")
   static class Entity2 {

      @Field
      private final String attribute;

      public Entity2(String attribute) {
         this.attribute = attribute;
      }

      public String getAttribute() {
         return attribute;
      }
   }
}
