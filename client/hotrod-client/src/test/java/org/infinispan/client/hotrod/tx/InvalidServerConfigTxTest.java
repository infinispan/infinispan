package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

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
      String name = method.getName();
      cacheManager.defineConfiguration(name, builder.build());
      remoteCacheManager.getConfiguration().addRemoteCache(name, c -> {
         c.transactionMode(TransactionMode.NONE);
      });
      assertFalse(remoteCacheManager.isTransactional(name));
      RemoteCache<String, String> cache = remoteCacheManager.getCache(name);
      assertFalse(cache.isTransactional());

   }

   public void testReadCommitted(Method method) {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      String name = method.getName();
      cacheManager.defineConfiguration(name, builder.build());
      remoteCacheManager.getConfiguration().addRemoteCache(name, c -> {
         c.transactionMode(TransactionMode.NONE);
      });
      assertFalse(remoteCacheManager.isTransactional(name));
      RemoteCache<String, String> cache = remoteCacheManager.getCache(name);
      assertFalse(cache.isTransactional());
   }

   public void testOkConfig(Method method) throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      String name = method.getName();
      cacheManager.defineConfiguration(name, builder.build());
      remoteCacheManager.getConfiguration().addRemoteCache(name, c -> {
         c.transactionMode(TransactionMode.NON_XA);
      });

      assertTrue(remoteCacheManager.isTransactional(name));
      RemoteCache<String, String> cache = remoteCacheManager.getCache(name);
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
