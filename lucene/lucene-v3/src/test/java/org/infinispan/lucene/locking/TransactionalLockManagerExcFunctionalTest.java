package org.infinispan.lucene.locking;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Tests verifying that the instantiation of TransactionalLockFactory in case of NON_TRANSACTIONAL cache fails.
 * The cases with started & stopped caches are checked.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.locking.TransactionalLockManagerExcFunctionalTest")
public class TransactionalLockManagerExcFunctionalTest extends TransactionalLockManagerFunctionalTest {

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Failed looking up TransactionManager: the cache is not running")
   public void testLuceneIndexLockingWithStoppedCache() throws IOException {
      final String commonIndexName = "myIndex";

      Cache cache1 = cache(0, "lucene");

      cache(0, "lucene").stop();
      cache(1, "lucene").stop();
      TestingUtil.killCacheManagers(cacheManagers);

      makeLockFactory(cache1, commonIndexName);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Failed looking up TransactionManager. Check if any transaction manager is associated with Infinispan cache: 'lucene'")
   public void testLuceneIndexLockingWithCache() throws IOException {
      final String commonIndexName = "myIndex";

      Cache cache1 = cache(0, "lucene");
      makeLockFactory(cache1, commonIndexName);
   }

   @Test(dataProvider = "writeLockNameProvider", enabled=false) @Override
   public void testLuceneIndexLocking(final String writeLockProvider) throws IOException {
      //do nothing
   }

   @Override
   protected TransactionMode getTransactionsMode() {
      return TransactionMode.NON_TRANSACTIONAL;
   }
}
