package org.infinispan.lucene.locking;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * LockManagerFunctionalTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.locking.LockManagerFunctionalTest")
public class LockManagerFunctionalTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createTestConfiguration(getTransactionsMode());
      createClusteredCaches(2, "lucene", configurationBuilder);
   }

   protected TransactionMode getTransactionsMode() {
      return TransactionMode.NON_TRANSACTIONAL;
   }

   @Test(dataProvider = "writeLockNameProvider")
   public void testLuceneIndexLocking(final String writeLockProvider) throws IOException {
      final String commonIndexName = "myIndex";
      LockFactory lockManagerA = makeLockFactory(cache(0,"lucene"), commonIndexName);
      LockFactory lockManagerB = makeLockFactory(cache(1, "lucene"), commonIndexName);
      LockFactory isolatedLockManager = makeLockFactory(cache(0, "lucene"), "anotherIndex");
      Lock luceneLockA = lockManagerA.makeLock(writeLockProvider);
      Lock luceneLockB = lockManagerB.makeLock(writeLockProvider);
      Lock anotherLock = isolatedLockManager.makeLock(writeLockProvider);

      assert luceneLockA.obtain();
      assert luceneLockB.isLocked();
      assert ! anotherLock.isLocked();
      assert ! luceneLockA.obtain();
      assert ! luceneLockB.obtain();
      luceneLockA.release();
      assert ! luceneLockB.isLocked();
      assert luceneLockB.obtain();
      lockManagerA.clearLock(writeLockProvider);
      assert ! luceneLockB.isLocked();
   }

   @DataProvider(name = "writeLockNameProvider")
   public Object[][] provideWriteLockName() {
      return new Object[][] {{IndexWriter.WRITE_LOCK_NAME}, {"SomeTestLockName"}};
   }

   protected LockFactory makeLockFactory(Cache<?,?> cache, String commonIndexName) {
      return new BaseLockFactory(cache, commonIndexName);
   }

}
