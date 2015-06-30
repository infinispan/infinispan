package org.infinispan.lucene.locking;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.impl.BaseLockFactory;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

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

   @Test(dataProvider = "writeLockNameProvider")
   public void testLuceneIndexLocking(final String writeLockProvider) throws IOException {
      BaseLockFactory baseLockFactory = BaseLockFactory.INSTANCE;
      Lock luceneLockA = baseLockFactory.makeLock(directory1, writeLockProvider);
      Lock luceneLockB = baseLockFactory.makeLock(directory1, writeLockProvider);
      Lock anotherLock = baseLockFactory.makeLock(directory3, writeLockProvider);

      assertTrue(luceneLockA.obtain());
      assertTrue(luceneLockB.isLocked());
      assertFalse(anotherLock.isLocked());
      assertFalse(luceneLockA.obtain());
      assertFalse(luceneLockB.obtain());

      luceneLockA.close();
      assertFalse(luceneLockB.isLocked());
      assertTrue(luceneLockB.obtain());

      luceneLockA.close();
      assertFalse(luceneLockB.isLocked());
   }

   @DataProvider(name = "writeLockNameProvider")
   public Object[][] provideWriteLockName() {
      return new Object[][] {{IndexWriter.WRITE_LOCK_NAME}, {"SomeTestLockName"}};
   }

   protected LockFactory makeLockFactory() {
      return BaseLockFactory.INSTANCE;
   }

}
