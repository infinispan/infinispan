package org.infinispan.lucene.readlocks;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.DirectoryIntegrityCheck;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * DistributedSegmentReadLockerTest represents a quick check on the functionality
 * of {@link org.infinispan.lucene.readlocks.DistributedSegmentReadLocker}
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@Test(groups = "functional", testName = "lucene.readlocks.DistributedSegmentReadLockerTest")
public class DistributedSegmentReadLockerTest extends MultipleCacheManagersTest {

   /** The Index name */
   protected static final String INDEX_NAME = "indexName";
   /** The cache name */
   protected static final String CACHE_NAME = "lucene";
   /** Chunk Size **/
   protected static final int CHUNK_SIZE = 6;
   /** The name of the test file **/
   protected static final String filename = "readme.txt";

   protected Cache cache0;
   protected Cache cache1;
   protected Directory dirA;
   protected Directory dirB;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration(TransactionMode.NON_TRANSACTIONAL);
      createClusteredCaches(2, CACHE_NAME, configurationBuilder);
   }

   @BeforeMethod
   protected void prepare() throws IOException {
      cache0 = cache(0, CACHE_NAME);
      cache1 = cache(1, CACHE_NAME);
      dirA = createDirectory(cache0);
      dirB = createDirectory(cache1);
      CacheTestSupport.initializeDirectory(dirA);
   }

   @Test
   public void testIndexWritingAndFinding() throws IOException, InterruptedException {
      verifyBoth(cache0,cache1);
      IndexOutput indexOutput = dirA.createOutput(filename, IOContext.DEFAULT);
      indexOutput.writeString("no need to write, nobody ever will read this");
      indexOutput.flush();
      indexOutput.close();
      assertFileExistsHavingRLCount(filename, 1, true);
      IndexInput openInput = dirB.openInput(filename, IOContext.DEFAULT);
      assertFileExistsHavingRLCount(filename, 2, true);
      dirA.deleteFile(filename);
      assertFileExistsHavingRLCount(filename, 1, false);
      //Lucene does use clone() - lock implementation ignores it as a clone is
      //cast on locked segments and released before the close on the parent object
      IndexInput clone = (IndexInput) openInput.clone();
      assertFileExistsHavingRLCount(filename, 1, false);
      clone.close();
      assertFileExistsHavingRLCount(filename, 1, false);
      openInput.close();
      assertFileNotExists(filename);
      dirA.close();
      dirB.close();
      verifyBoth(cache0, cache1);
   }

   void assertFileNotExists(String fileName) throws InterruptedException {
      DirectoryIntegrityCheck.assertFileNotExists(cache0, INDEX_NAME, fileName, 10000L);
      DirectoryIntegrityCheck.assertFileNotExists(cache1, INDEX_NAME, fileName, 10000L);
   }

   void assertFileExistsHavingRLCount(String fileName, int expectedReadcount, boolean expectRegisteredInFat) {
      DirectoryIntegrityCheck.assertFileExistsHavingRLCount(cache0, fileName, INDEX_NAME, expectedReadcount, CHUNK_SIZE, expectRegisteredInFat);
      DirectoryIntegrityCheck.assertFileExistsHavingRLCount(cache1, fileName, INDEX_NAME, expectedReadcount, CHUNK_SIZE, expectRegisteredInFat);
   }

   Directory createDirectory(Cache cache) {
      return DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME)
             .chunkSize(CHUNK_SIZE)
             .overrideSegmentReadLocker(new DistributedSegmentReadLocker(cache, INDEX_NAME))
             .create();
   }

   void verifyBoth(Cache cache0, Cache cache1) {
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache0, INDEX_NAME);
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache1, INDEX_NAME);
   }

}
