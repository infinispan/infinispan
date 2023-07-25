package org.infinispan.jupiter;


import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.locks.LockManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

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
      if (cache == null && SecurityActions.getCacheManagerConfiguration(cacheManager).defaultCacheName().isPresent()) {
         cache = cacheManager.getCache();
      }
   }

   protected void teardown() {
      TestingUtil.clearContent(cacheManager);
      TestingUtil.killCacheManagers(cacheManager);
      cache = null;
      cacheManager = null;
   }

   protected void clearCacheManager() {
      TestingUtil.clearContent(cacheManager);
   }

   @BeforeAll
   protected void createBeforeClass() throws Exception {
      try {
         if (cleanupAfterTest()) setup();
         else assert cleanupAfterMethod() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @BeforeEach
   protected void createBeforeMethod() throws Exception {
      try {
         if (cleanupAfterMethod()) setup();
         else assert cleanupAfterTest() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @AfterAll
   protected void destroyAfterClass() {
      try {
         if (cleanupAfterTest()) teardown();
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   @AfterEach
   protected void destroyAfterMethod() {
      if (cleanupAfterMethod()) teardown();
   }

   @AfterEach
   protected void clearContent() {
      if (cleanupAfterTest()) clearCacheManager();
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
      assertNoTransactions(cache);
   }

   protected void assertNoTransactions(Cache<?, ?> cache) {
      eventually(() -> {
         int localTxCount = TestingUtil.extractComponent(cache, TransactionTable.class).getLocalTxCount();
         if (localTxCount != 0) {
            log.tracef("Local tx=%s", localTxCount);
            return false;
         }
         return true;
      });
   }
}
