package org.infinispan.client.hotrod.tx;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.assertNoTransaction;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
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

   public void testNonTxCache(Method method) throws SystemException, NotSupportedException {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA);
      final TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      try {
         Exceptions.expectException(HotRodClientException.class, "ISPN004084.*", () -> cache.put("k1", "v1"));
      } finally {
         tm.rollback();
      }
      assertNoTransaction(remoteCacheManager);
   }

   public void testReadCommitted(Method method) throws SystemException, NotSupportedException {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA);
      final TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      try {
         Exceptions.expectException(HotRodClientException.class, "ISPN004084.*", () -> cache.put("k1", "v1"));
      } finally {
         tm.rollback();
      }
      assertNoTransaction(remoteCacheManager);
   }

   public void testOptimistic(Method method) throws SystemException, NotSupportedException {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA);
      final TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      try {
         Exceptions.expectException(HotRodClientException.class, "ISPN004084.*", () -> cache.put("k1", "v1"));
      } finally {
         tm.rollback();
      }
      assertNoTransaction(remoteCacheManager);
   }

   public void testOkConfig(Method method) throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheManager.defineConfiguration(method.getName(), builder.build());

      RemoteCache<String, String> cache = remoteCacheManager.getCache(method.getName(), TransactionMode.NON_XA);
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
