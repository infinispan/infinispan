package org.infinispan.lucene.locking;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.impl.BaseLockFactory;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * LockManagerFunctionalTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.locking.LockManagerFunctionalTest")
public class LockManagerFunctionalTest extends MultipleCacheManagersTest {

   private static final String COMMON_INDEX_NAME = "myIndex";
   private static final String ANOTHER_INDEX = "commonIndexName";
   protected Directory directory1;
   protected Directory directory2;
   protected Directory directory3;

   protected void createCacheManagers() {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration(getTransactionsMode());
      createClusteredCaches(2, "lucene", configurationBuilder);
      directory1 = createDirectory(cache(0,"lucene"), COMMON_INDEX_NAME);
      directory2 = createDirectory(cache(1,"lucene"), COMMON_INDEX_NAME);
      directory3 = createDirectory(cache(0,"lucene"), ANOTHER_INDEX);
   }

   private Directory createDirectory(Cache lockCache, String indexName) {
      return DirectoryBuilder.newDirectoryInstance(
              lockCache,
              lockCache,
              lockCache,
              indexName).create();
   }

   protected TransactionMode getTransactionsMode() {
      return TransactionMode.NON_TRANSACTIONAL;
   }

   @Test
   public void testLuceneIndexLocking() throws IOException {
      assertFalse(isLocked(directory1));
      Lock obtainedLock = directory1.obtainLock(IndexWriter.WRITE_LOCK_NAME);
      assertTrue(isLocked(directory1));
      assertTrue(isLocked(directory2));
      assertFalse(isLocked(directory3));
      obtainedLock.ensureValid(); //will throw an exception on failure
      obtainedLock.close();
      assertFalse(isLocked(directory1));
      assertFalse(isLocked(directory2));
      assertFalse(isLocked(directory3));
   }

   //Replacing the now deprecated IndexWriter.isLocked helper
   private static boolean isLocked(Directory directory) throws IOException {
      try {
         directory.obtainLock(IndexWriter.WRITE_LOCK_NAME).close();
         return false;
      } catch (LockObtainFailedException failed) {
         return true;
      }
   }

   protected LockFactory makeLockFactory() {
      return BaseLockFactory.INSTANCE;
   }

}
