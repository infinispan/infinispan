package org.infinispan.api.batch;

import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.getTransactionManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedBaseTransactionManager;
import org.testng.annotations.Test;


@Test(groups = {"functional", "transaction"}, testName = "api.batch.BatchWithCustomTMTest")
public class BatchWithCustomTMTest extends AbstractBatchTest {

   public void testBatchWithOngoingTM(Method method) throws Exception {
      Cache<String, String> cache = createCache(method.getName());
      TransactionManager tm = getTransactionManager(cache);
      assertEquals(MyDummyTransactionManager.class, tm.getClass());
      tm.begin();
      cache.put("k", "v");
      cache.startBatch();
      cache.put("k2", "v2");
      tm.commit();

      assertEquals("v", cache.get("k"));
      assertEquals("v2", cache.get("k2"));

      cache.endBatch(false); // should be a no op
      assertEquals("v", cache.get("k"));
      assertEquals("v2", cache.get("k2"));
   }

   public void testBatchWithoutOngoingTMSuspension(Method method) throws Exception {
      Cache<String, String> cache = createCache(method.getName());
      TransactionManager tm = getTransactionManager(cache);
      assertEquals(MyDummyTransactionManager.class, tm.getClass());
      assertNoTransaction(tm);
      cache.startBatch();

      cache.put("k", "v");
      assertNoTransaction(tm);
      cache.put("k2", "v2");

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      expectException(IllegalStateException.class, tm::commit);

      assertNoTransaction(tm);

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      cache.endBatch(true); // should be a no op

      assertEquals("v", getOnDifferentThread(cache, "k"));
      assertEquals("v2", getOnDifferentThread(cache, "k2"));
   }

   public void testBatchRollback(Method method) throws Exception {
      Cache<String, String> cache = createCache(method.getName());
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      cache.endBatch(false);

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));
   }

   protected <K, V> Cache<K, V> createCache(String name) {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.transaction().transactionManagerLookup(new MyDummyTransactionManagerLookup());
      c.invocationBatching().enable();
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      cacheManager.defineConfiguration(name, c.build());
      return cacheManager.getCache(name);
   }

   static class MyDummyTransactionManagerLookup extends EmbeddedTransactionManagerLookup {
      MyDummyTransactionManager tm = new MyDummyTransactionManager();

      @Override
      public TransactionManager getTransactionManager() {
         return tm;
      }
   }

   static class MyDummyTransactionManager extends EmbeddedBaseTransactionManager {

   }
}
