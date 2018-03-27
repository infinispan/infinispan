package org.infinispan.api.batch;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.concurrent.Future;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "transaction", "smoke"})
public abstract class AbstractBatchTest extends SingleCacheManagerTest {

   @Override
   public EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   public void testClearInBatch(Method method) {
      //tests if the clear doesn't leak the batch transaction.
      //if it does, the get() will be executed against a committed transaction and it will fail.
      Cache<String, String> cache = createCache(method.getName());
      cache.put("k2", "v2");

      cache.startBatch();
      cache.clear();
      cache.put("k1", "v1");
      cache.endBatch(true);

      assertEquals(null, cache.get("k2"));
      assertEquals("v1", cache.get("k1"));
   }

   public void testPutForExternalReadInBatch(Method method) {
      //tests if the putForExternalRead doesn't leak the batch transaction.
      //if it does, the get() will be executed against a committed transaction and it will fail.
      Cache<String, String> cache = createCache(method.getName());

      cache.startBatch();
      cache.putForExternalRead("k1", "v1");
      cache.put("k2", "v2");
      cache.endBatch(true);

      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));

      cache.startBatch();
      cache.putForExternalRead("k3", "v3");
      cache.put("k1", "v2");
      cache.endBatch(false);

      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
      assertEquals("v3", cache.get("k3"));
   }

   String getOnDifferentThread(final Cache<String, String> cache, final String key) throws Exception {
      Future<String> f = fork(() -> {
         cache.startBatch();
         String v = cache.get(key);
         cache.endBatch(true);
         return v;
      });
      return f.get();
   }

   void assertNoTransaction(TransactionManager transactionManager) throws SystemException {
      assertNull("Should have no ongoing txs", transactionManager.getTransaction());
   }

   protected abstract <K, V> Cache<K, V> createCache(String name);
}
