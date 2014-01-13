package org.infinispan.lucene;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.lucene.readlocks.DistributedSegmentReadLocker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests covering InfinispanDirectory simple use cases, like:
 *   - InfinispanDirectory object initialization with illegal arguments.
 *   - InfinispanDirectory object initialization with proper arguments.
 *   - Tests are added testing the touchFile, fileModified, renameFile methods.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.InfinispanDirectoryTest")
public class InfinispanDirectoryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = CacheTestSupport.createLocalCacheConfiguration();

      return TestCacheManagerFactory.createCacheManager(configuration);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "chunkSize must be a positive integer")
   public void testInitWithInvalidChunkSize() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "index")
         .overrideSegmentReadLocker(new DistributedSegmentReadLocker(cache, cache, cache, "index"))
         .chunkSize(0);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidCache() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(null, cache, cache, "indexName");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidChunkCache() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(cache, null, cache, "indexName");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidIndexName() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidLockFactory() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName")
         .overrideWriteLocker(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInitWithInvalidSegmentReadLocker() {
      Cache cache = cacheManager.getCache();
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName")
         .overrideSegmentReadLocker(null);
   }

   @Test
   public void testFileModified() throws Exception {
      Directory dir = null;
      try {
         Cache cache = cacheManager.getCache();
         String fileName = "dummyFileName";
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
         createFile(fileName, dir);

         assert dir.fileExists(fileName);
         assert dir.fileModified(fileName) != 0;

         assert dir.fileModified("nonExistentFileName.txt") == 0;
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testTouchFile() throws Exception {
      Directory dir = null;
      try {
         Cache cache = cacheManager.getCache();
         String fileName = "testfile.txt";

         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
         createFile(fileName, dir);

         long lastModifiedDate = dir.fileModified(fileName);

         Thread.sleep(100);

         dir.touchFile(fileName);
         assert lastModifiedDate != dir.fileModified(fileName);
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testTouchNonExistentFile() throws Exception {
      Directory dir = null;
      try {
         Cache cache = cacheManager.getCache();
         String fileName = "nonExistent.txt";
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();

         long lastModifiedDate = dir.fileModified(fileName);
         Thread.sleep(100);

         dir.touchFile(fileName);
         AssertJUnit.assertEquals(lastModifiedDate, dir.fileModified(fileName));
      } finally {
         dir.close();
      }
   }

   @Test
   public void testRenameFile() throws Exception {
      Directory dir = null;
      try {
         Cache cache = cacheManager.getCache();
         String fileName = "testfile.txt";
         String newFileName = "newtestfile.txt";

         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
         createFile(fileName, dir);

         ((DirectoryExtensions) dir).renameFile(fileName, newFileName);

         assert !dir.fileExists(fileName);
         assert dir.fileExists(newFileName);
      } finally {
         if (dir != null) dir.close();
      }
   }

   @Test
   public void testFileLength() throws IOException {
      Directory dir = null;
      try {
         dir = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").create();
         AssertJUnit.assertEquals(0, dir.fileLength("nonExistentFile.txt"));
      } finally {
         if (dir != null) dir.close();
      }
   }

   private void createFile(final String fileName, final Directory dir) throws IOException {
      IndexOutput io = null;

      try {
         io = dir.createOutput(fileName);

         io.writeByte((byte) 66);
         io.writeByte((byte) 69);
      } finally {
         io.flush();
         io.close();
      }
   }

   private void verifyDir(final Directory dir, final String expectedIndexName) throws IOException {
      DirectoryExtensions infDir = (DirectoryExtensions) dir;
      AssertJUnit.assertEquals(expectedIndexName, infDir.getIndexName());

      writeTextToIndex(dir, 0, "hi all");
      assertTextIsFoundInIds(dir, "hi", 0);
      writeTextToIndex(dir, 1, "all together");
      assertTextIsFoundInIds(dir, "all", 0, 1);
   }
}
