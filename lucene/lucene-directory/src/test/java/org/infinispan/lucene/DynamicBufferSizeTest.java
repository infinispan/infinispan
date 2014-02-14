package org.infinispan.lucene;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * The chunk size might be changed after a segment has been created.
 * This test verifies an existing index is still readable.
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "lucene.DynamicBufferSizeTest")
public class DynamicBufferSizeTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.LOCAL)
            .invocationBatching().enable();
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Test
   public void roundingTest() {
      FileMetadata m = new FileMetadata(10);
      AssertJUnit.assertEquals(0, m.getNumberOfChunks());
      m.setSize(10);
      AssertJUnit.assertEquals(1, m.getNumberOfChunks());
      m.setSize(11);
      AssertJUnit.assertEquals(2, m.getNumberOfChunks());
      m = new FileMetadata(11);
      m.setSize(11);
      AssertJUnit.assertEquals(1, m.getNumberOfChunks());
      m.setSize(22);
      AssertJUnit.assertEquals(2, m.getNumberOfChunks());
      m.setSize(31);
      m = new FileMetadata(10);
      m.setSize(31);
      AssertJUnit.assertEquals(4, m.getNumberOfChunks());
   }

   @Test
   public void testReadingFromDifferentlySizedBuffers() throws IOException {
      cache = cacheManager.getCache();
      Directory dirA = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").chunkSize(7).create();
      writeTextToIndex(dirA, 0, "hi from node A");
      Directory dirB = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "indexName").chunkSize(8).create();
      assertTextIsFoundInIds(dirB, "hi", 0);
      writeTextToIndex(dirB, 1, "index B is sharing the same index but using a differently sized chunk size");
      assertTextIsFoundInIds(dirA, "size", 1);
   }

   @Test
   public void testFileMetaData() {
      FileMetadata data1 = new FileMetadata(1024);
      FileMetadata data2 = new FileMetadata(2048);
      FileMetadata data3 = new FileMetadata(1024);

      FileMetadata data4 = data1;

      data1.touch();
      data2.touch();

      assert !data1.equals(new FileCacheKey("testIndex", "testFile"));
      AssertJUnit.assertNotNull(data1);
      assert data1.equals(data4);
      assert !data1.equals(data3);

      data3.setLastModified(data1.getLastModified());
      assert data1.equals(data3);

      data3.setSize(2048);
      assert !data1.equals(data3);

      data2.setLastModified(data1.getLastModified());
      assert !data1.equals(data2);

      AssertJUnit.assertEquals("FileMetadata{" + "lastModified=" + data1.getLastModified() + ", size=" + data1.getSize() + '}', data1.toString());
   }
}
