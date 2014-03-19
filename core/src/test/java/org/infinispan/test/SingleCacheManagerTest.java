package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Base class for tests that operate on a single (most likely local) cache instance. This operates similar to {@link
 * org.infinispan.test.MultipleCacheManagersTest}, but on only one CacheManager.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.test.MultipleCacheManagersTest
 */
public abstract class SingleCacheManagerTest extends AbstractCacheTest {

   protected EmbeddedCacheManager cacheManager;
   protected Cache<Object, Object> cache;

   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      if (cache == null) cache = cacheManager.getCache();
   }

   protected void teardown() {
      TestingUtil.killCacheManagers(cacheManager);
      cache = null;
      cacheManager = null;
   }

   @BeforeClass(alwaysRun = true)
   protected void createBeforeClass() throws Exception {
      try {
         if (cleanupAfterTest()) setup();
         else assert cleanupAfterMethod() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @BeforeMethod(alwaysRun = true)
   protected void createBeforeMethod() throws Exception {
      try {
         if (cleanupAfterMethod()) setup();
         else assert cleanupAfterTest() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      try {
         if (cleanupAfterTest()) teardown();
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   @AfterMethod(alwaysRun=true)
   protected void destroyAfterMethod() {
      if (cleanupAfterMethod()) teardown();
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() {
      if (cleanupAfterTest()) TestingUtil.clearContent(cacheManager);
   }

   protected ConfigurationBuilder getDefaultStandaloneCacheConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultCacheConfiguration(transactional);
   }

   protected TransactionManager tm() {
      return cache.getAdvancedCache().getTransactionManager();
   }

   protected Transaction tx() {
      try {
         return cache.getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected LockManager lockManager(String cacheName) {
      return TestingUtil.extractLockManager(cacheManager.getCache(cacheName));
   }

   protected LockManager lockManager() {
      return TestingUtil.extractLockManager(cache);
   }


   protected abstract EmbeddedCacheManager createCacheManager() throws Exception;

   @SuppressWarnings("unchecked")
   protected <K,V> Cache<K, V> cache() {
      return (Cache<K, V>)cache;
   }

   protected <K,V> Cache<K, V> cache(String name) {
      return cacheManager.getCache(name);
   }

   protected void assertNoTransactions() {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int localTxCount = TestingUtil.extractComponent(cache, TransactionTable.class).getLocalTxCount();
            if (localTxCount != 0) {
               log.tracef("Local tx=%s", localTxCount);
               return false;
            }
            return true;
         }
      });
   }
}
