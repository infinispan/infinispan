package org.infinispan.lucene.locking;

import java.io.IOException;

import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.lucene.impl.DirectoryBuilderImpl;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests verifying that the instantiation of TransactionalLockFactory in case of NON_TRANSACTIONAL cache fails.
 * The cases with started and stopped caches are checked.
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

      LockFactory lockFactory = makeLockFactory();

      lockFactory.obtainLock(new DirectoryBuilderImpl(cache1,cache1,cache1,commonIndexName).create(),"myLock");
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "Failed looking up TransactionManager. Check if any transaction manager is associated with Infinispan cache: 'lucene'")
   public void testLuceneIndexLockingWithCache() throws IOException {
      final String commonIndexName = "myIndex";

      Cache cache1 = cache(0, "lucene");
      LockFactory lockFactory = makeLockFactory();

      lockFactory.obtainLock(new DirectoryBuilderImpl(cache1,cache1,cache1,commonIndexName).create(),"myLock");
   }

   @Test @Override
   public void testLuceneIndexLocking() throws IOException {
      //do nothing
   }

   @Override
   protected TransactionMode getTransactionsMode() {
      return TransactionMode.NON_TRANSACTIONAL;
   }
}
