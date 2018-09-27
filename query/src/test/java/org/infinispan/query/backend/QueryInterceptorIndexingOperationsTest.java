package org.infinispan.query.backend;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.impl.ComponentRegistryUtils;
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
      Directory directory = initializeAndExtractDirectory(cache);

      Entity1 entity1 = new Entity1("e1");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, () -> cache.put(1, new Entity1("e2")));

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideNonIndexedByIndexed() throws IOException {
      Directory directory = initializeAndExtractDirectory(cache);

      cache.put(1, "string value");

      long commits = doRecordingCommits(directory, () -> cache.put(1, new Entity1("e1")));

      assertEquals(1, commits);
      assertEquals(1, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideIndexedByNonIndexed() throws IOException {
      Directory directory = initializeAndExtractDirectory(cache);

      final Entity1 entity1 = new Entity1("title");
      cache.put(1, entity1);

      long commits = doRecordingCommits(directory, () -> cache.put(1, "another"));

      assertEquals(1, commits);
      assertEquals(0, countIndexedDocuments(Entity1.class));
      assertEquals(0, countIndexedDocuments(Entity2.class));
   }

   @Test
   public void testOverrideIndexedByOtherIndexed() throws IOException {
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
      builder.indexing().index(Index.ALL)
            .addIndexedEntity(Entity1.class)
            .addIndexedEntity(Entity2.class)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      Configuration nonIndexed = nonIndexed();
      cm.defineConfiguration("LuceneIndexesMetadata", nonIndexed);
      cm.defineConfiguration("LuceneIndexesData", nonIndexed);
      cm.defineConfiguration("LuceneIndexesLocking", nonIndexed);
      return cm;
   }

   private Configuration nonIndexed() {
      return new ConfigurationBuilder().indexing().index(Index.NONE)
              .addProperty("lucene_version", "LUCENE_CURRENT").build();
   }

   interface Operation {
      void execute();
   }

   static long doRecordingCommits(Directory directory, Operation operation) throws IOException {
      long initialGen = SegmentInfos.getLastCommitGeneration(directory);
      operation.execute();
      return SegmentInfos.getLastCommitGeneration(directory) - initialGen;
   }

   private Directory initializeAndExtractDirectory(Cache cache) {
      SearchIntegrator searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
      DirectoryBasedIndexManager indexManager = (DirectoryBasedIndexManager) searchFactory.getIndexBindings().get(Entity1.class)
            .getIndexManagerSelector().all().iterator().next();
      return indexManager.getDirectoryProvider().getDirectory();
   }

   private int countIndexedDocuments(Class<?> clazz) {
      CacheQuery<?> query = Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), clazz);
      return query.list().size();
   }

   @Indexed(index = "theIndex")
   @SuppressWarnings("unused")
   static class Entity1 {

      @Field
      private final String attribute;

      public Entity1(String attribute) {
         this.attribute = attribute;
      }
   }

   @Indexed(index = "theIndex")
   @SuppressWarnings("unused")
   static class Entity2 {

      @Field
      private final String attribute;

      public Entity2(String attribute) {
         this.attribute = attribute;
      }
   }
}
