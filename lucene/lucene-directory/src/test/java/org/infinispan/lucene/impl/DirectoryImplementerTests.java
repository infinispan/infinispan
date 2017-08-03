package org.infinispan.lucene.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests covering DirectoryImplementor class.
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
                  public Lock obtainLock(Directory dir, String lockName) throws IOException {
                     return null;
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
         AssertJUnit.assertEquals(INDEX_NAME, ((DirectoryLucene) dir).getIndexName());
         AssertJUnit.assertEquals("InfinispanDirectory{indexName=\'" + INDEX_NAME + "\'}", dir.toString());

      } finally {
         if (dir != null) dir.close();
      }
   }

   private void createFile(Directory directory, String name) throws IOException {
      IndexOutput indexOutput = directory.createOutput(name, IOContext.DEFAULT);
      indexOutput.writeByte((byte) 0);
      indexOutput.close();
   }

   @Test
   public void testConfigureAsyncDeletes() throws Exception {
      Cache cache = cacheManager.getCache();

      ExecutorService executor = spy(Executors.newFixedThreadPool(1));

      try (Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
            .chunkSize(BUFFER_SIZE)
            .deleteOperationsExecutor(executor)
            .create()) {

         createFile(directory, "file");

         assertEquals(directory.listAll().length, 1);

         directory.deleteFile("file");

         eventuallyEquals(0, () -> {
            try {
               return directory.listAll().length;
            } catch (IOException e) {
               Assert.fail("Error inspecting directory", e);
               return null;
            }
         });

         verify(executor).execute(any(DirectoryLucene.DeleteTask.class));

      } finally {
         executor.shutdownNow();
      }
   }
}
