package org.infinispan.tx;

import static org.infinispan.testing.Exceptions.expectException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.MarkAsRollbackTest")
public class MarkAsRollbackTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));
      cache = cm.getCache();
      return cm;
   }

   public void testMarkAsRollbackAfterMods() throws Exception {

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assertNotNull(tm);
      tm.begin();
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
      tm.setRollbackOnly();
      expectException(RollbackException.class, () -> tm.commit());

      assertNull(tm.getTransaction(), "There should be no transaction in scope anymore!");
      assertNull(cache.get("k"), "Expected a null but was " + cache.get("k"));
   }

   public void testMarkAsRollbackBeforeMods() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assertNotNull(tm);
      tm.begin();
      tm.setRollbackOnly();
      expectException(CacheException.class, IllegalStateException.class, () -> cache.put("k", "v"));
      expectException(RollbackException.class, () -> tm.commit());

      assertNull(tm.getTransaction(), "There should be no transaction in scope anymore!");
      assertNull(cache.get("k"), "Expected a null but was " + cache.get("k"));
   }
}
