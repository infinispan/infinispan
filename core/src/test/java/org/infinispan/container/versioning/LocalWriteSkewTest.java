package org.infinispan.container.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Exceptions;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;

/**
 * Tests local-mode versioning
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Test(testName = "container.versioning.LocalWriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class LocalWriteSkewTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .transaction().lockingMode(LockingMode.OPTIMISTIC);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testWriteSkewEnabled() throws Exception {
      // Auto-commit is true

      cache.put("hello", "world 1");

      tm().begin();
      assertEquals("world 1", cache.get("hello"), "Wrong value read by transaction for key hello");
      Transaction t = tm().suspend();

      // Create a write skew
      cache.put("hello", "world 3");

      tm().resume(t);
      cache.put("hello", "world 2");

      Exceptions.expectException(RollbackException.class, tm()::commit);
      assertEquals("world 3", cache.get("hello"), "Wrong final value for key hello");
   }

   public void testWriteSkewMultiEntries() throws Exception {
      tm().begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      tm().commit();

      tm().begin();
      cache.put("k2", "v2000");
      assertEquals("v1", cache.get("k1"), "Wrong value read by transaction for key k1");
      assertEquals("v2000", cache.get("k2"), "Wrong value read by transaction for key k2");
      Transaction t = tm().suspend();

      // Create a write skew
      // Auto-commit is true
      cache.put("k1", "v3");

      tm().resume(t);
      cache.put("k1", "v5000");

      Exceptions.expectException(RollbackException.class, tm()::commit);

      assertEquals("v3", cache.get("k1"), "Wrong final value for key k1");
      assertEquals("v2", cache.get("k2"), "Wrong final value for key k2");
   }

   public void testNullEntries() throws Exception {
      // Auto-commit is true
      cache.put("hello", "world");

      tm().begin();
      assertEquals("world", cache.get("hello"), "Wrong value read by transaction for key hello");
      Transaction t = tm().suspend();

      cache.remove("hello");

      assertNull(cache.get("hello"), "Wrong value after remove for key hello");

      tm().resume(t);
      cache.put("hello", "world2");

      Exceptions.expectException(RollbackException.class, tm()::commit);
      assertNull(cache.get("hello"), "Wrong final value for key hello");
   }

   public void testSameNodeKeyCreation() throws Exception {
      tm().begin();
      assertNull(cache.get("NewKey"), "Wrong value read by transaction 1 for key NewKey");
      cache.put("NewKey", "v1");
      Transaction tx0 = tm().suspend();

      //other transaction do the same thing
      tm().begin();
      assertNull(cache.get("NewKey"), "Wrong value read by transaction 2 for key NewKey");
      cache.put("NewKey", "v2");
      tm().commit();

      tm().resume(tx0);
      Exceptions.expectException(RollbackException.class, tm()::commit);

      assertEquals("v2", cache.get("NewKey"), "Wrong final value for key NewKey");
   }
}
