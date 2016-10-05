package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @since 7.2
 */
@Test (groups = "functional", testName = "api.APITxTest")
public class APITxTest extends APINonTxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testSizeInExplicitTxWithNonExistent() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         cache.get("no-exist");
         assertEquals(1, cache.size());
         cache.put("no-exist", "value");
         assertEquals(2, cache.size());
      } finally {
         tm1.rollback();
      }
   }

   public void testSizeInExplicitTxWithRemoveNonExistent() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         cache.remove("no-exist");
         assertEquals(1, cache.size());
         cache.put("no-exist", "value");
         assertEquals(2, cache.size());
      } finally {
         tm1.rollback();
      }
   }

   public void testSizeInExplicitTxWithRemoveExistent() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         cache.put("exist", "value");
         assertEquals(2, cache.size());
         cache.remove("exist");
         assertEquals(1, cache.size());
      } finally {
         tm1.rollback();
      }
   }

   public void testSizeInExplicitTx() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k", "v");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         assertEquals(1, cache.size());
      } finally {
         tm1.rollback();
      }
   }

   public void testSizeInExplicitTxWithModification() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         cache.put("k2", "v2");
         assertEquals(2, cache.size());
      } finally {
         tm1.rollback();
      }
   }

   public void testEntrySetIteratorRemoveInExplicitTx() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         try (CloseableIterator<Map.Entry<Object, Object>> entryIterator = cache.entrySet().iterator()) {
            entryIterator.next();
            entryIterator.remove();
            assertEquals(0, cache.size());
         }
      } finally {
         tm1.rollback();
      }
   }

   public void testKeySetIteratorRemoveInExplicitTx() throws SystemException, NotSupportedException {
      assertEquals(0, cache.size());
      cache.put("k1", "v1");

      TransactionManager tm1 = TestingUtil.getTransactionManager(cache);
      tm1.begin();
      try {
         try (CloseableIterator<Object> entryIterator = cache.keySet().iterator()) {
            entryIterator.next();
            entryIterator.remove();
            assertEquals(0, cache.size());
         }
      } finally {
         tm1.rollback();
      }
   }

}
