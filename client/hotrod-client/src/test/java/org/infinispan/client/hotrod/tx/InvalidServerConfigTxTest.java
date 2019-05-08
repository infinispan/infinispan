package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.exceptions.CacheNotTransactionalException;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Checks invalid server configuration.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.tx.InvalidServerConfigTxTest")
public class InvalidServerConfigTxTest extends SingleHotRodServerTest {

   public void testNonTxCache(Method method) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      assertFalse(remoteCacheManager.isTransactional(method.getName()));
      Exceptions.expectException(CacheNotTransactionalException.class, "ISPN004084.*",
            () -> remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA));

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NONE);
      assertFalse(cache.isTransactional());

   }

   public void testReadCommitted(Method method) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      assertFalse(remoteCacheManager.isTransactional(method.getName()));
      Exceptions.expectException(CacheNotTransactionalException.class, "ISPN004084.*",
            () -> remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA));

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NONE);
      assertFalse(cache.isTransactional());
   }

   public void testOptimistic(Method method) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      assertFalse(remoteCacheManager.isTransactional(method.getName()));
      Exceptions.expectException(CacheNotTransactionalException.class, "ISPN004084.*",
            () -> remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA));

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NONE);
      assertFalse(cache.isTransactional());
   }

   public void testOkConfig(Method method) throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      assertTrue(remoteCacheManager.isTransactional(method.getName()));
      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA);
      assertTrue(cache.isTransactional());

      final TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      try {
         cache.put("k1", "v1");
      } finally {
         tm.commit();
      }
      assertEquals("v1", cache.get("k1"));
      assertNoTransaction(remoteCacheManager);
   }
}
