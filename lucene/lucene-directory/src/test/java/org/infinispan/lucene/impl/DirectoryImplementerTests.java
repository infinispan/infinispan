package org.infinispan.lucene.impl;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Version;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Tests covering DirecotoryImplementor class.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.DirectoryImplementerTests")
public class DirectoryImplementerTests extends SingleCacheManagerTest {

   private static final String INDEX_NAME = "index-A";

   private static final int BUFFER_SIZE = 1024;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = CacheTestSupport.createLocalCacheConfiguration();

      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "chunkSize must be a positive integer")
   public void testInitWithInvalidChunkSize() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(0).create();
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testFailureOfOverrideWriteLocker() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE)
               .overrideWriteLocker(null)
               .create();
      } finally {
         if (dir != null) dir.close();
      }

   }

   public void testOverrideWriteLocker() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE)
               .overrideWriteLocker(new LockFactory() {
                  @Override
                  public Lock makeLock(String lockName) {
                     return null;
                  }

                  @Override
                  public void clearLock(String lockName) throws IOException {

                  }
               })
               .create();

         AssertJUnit.assertEquals(0, dir.listAll().length);
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testGetIndexNameAndToString() throws IOException {
      Cache cache = cacheManager.getCache();
      Directory dir = null;

      try {

         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).chunkSize(BUFFER_SIZE).create();
         AssertJUnit.assertEquals(INDEX_NAME, ((DirectoryLuceneV4) dir).getIndexName());
         AssertJUnit.assertEquals("InfinispanDirectory{indexName=\'" + INDEX_NAME + "\'}", dir.toString());

      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testConfigureAsyncDeletes() throws Exception {
      Cache cache = cacheManager.getCache();
      Directory dir = null;
      Version version = Version.LUCENE_4_10_1;
      IndexWriterConfig iwc = new IndexWriterConfig(version, new StandardAnalyzer());
      iwc.setMaxBufferedDocs(2);
      IndexWriter indexWriter = null;
      Document document = new Document();
      document.add(new TextField("field", "whatever", Field.Store.YES));
      TrackingThreadPoolExecutor executorService = new TrackingThreadPoolExecutor();
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
               .chunkSize(BUFFER_SIZE)
               .deleteOperationsExecutor(executorService)
               .create();

         indexWriter = new IndexWriter(dir, iwc);
         indexWriter.addDocument(document);
         indexWriter.commit();

         indexWriter.addDocument(document);
         indexWriter.commit();

         executorService.shutdown();

         AssertJUnit.assertTrue(executorService.isSegmentDeleted("0"));
         AssertJUnit.assertTrue(executorService.isSegmentDeleted("1"));

      } finally {
         if (indexWriter != null) indexWriter.close();
         if (dir != null) dir.close();
      }
   }

   class TrackingThreadPoolExecutor extends ThreadPoolExecutor {
      private final Set<String> deletedSegments = new ConcurrentHashSet<>();

      TrackingThreadPoolExecutor() {
         super(0, 5, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new CallerRunsPolicy());
      }

      private String extractSegmentName(String fileName) {
         if (!fileName.startsWith("_")) {
            return null;
         } else {
            return fileName.substring(1, fileName.indexOf('.'));
         }
      }

      @Override
      protected void afterExecute(Runnable r, Throwable t) {
         DirectoryLuceneV4.DeleteTask task = (DirectoryLuceneV4.DeleteTask) r;
         String name = task.getFileName();
         String segment = extractSegmentName(name);
         if (segment != null) {
            deletedSegments.add(segment);
         }
      }

      public boolean isSegmentDeleted(String segmentName) {
         return deletedSegments.contains(segmentName);
      }
   }
}
