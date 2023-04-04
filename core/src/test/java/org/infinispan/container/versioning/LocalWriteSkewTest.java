package org.infinispan.container.versioning;

import static org.testng.AssertJUnit.assertEquals;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

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
      assertEquals("Wrong value read by transaction for key hello", "world 1", cache.get("hello"));
      Transaction t = tm().suspend();

      // Create a write skew
      cache.put("hello", "world 3");

      tm().resume(t);
      cache.put("hello", "world 2");

      Exceptions.expectException(RollbackException.class, tm()::commit);
      assertEquals("Wrong final value for key hello", "world 3", cache.get("hello"));
   }

   public void testWriteSkewMultiEntries() throws Exception {
      tm().begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      tm().commit();

      tm().begin();
      cache.put("k2", "v2000");
      assertEquals("Wrong value read by transaction for key k1", "v1", cache.get("k1"));
      assertEquals("Wrong value read by transaction for key k2", "v2000", cache.get("k2"));
      Transaction t = tm().suspend();

      // Create a write skew
      // Auto-commit is true
      cache.put("k1", "v3");

      tm().resume(t);
      cache.put("k1", "v5000");

      Exceptions.expectException(RollbackException.class, tm()::commit);

      assertEquals("Wrong final value for key k1", "v3", cache.get("k1"));
      assertEquals("Wrong final value for key k2", "v2", cache.get("k2"));
   }

   public void testNullEntries() throws Exception {
      // Auto-commit is true
      cache.put("hello", "world");

      tm().begin();
      assertEquals("Wrong value read by transaction for key hello", "world", cache.get("hello"));
      Transaction t = tm().suspend();

      cache.remove("hello");

      assertEquals("Wrong value after remove for key hello", null, cache.get("hello"));

      tm().resume(t);
      cache.put("hello", "world2");

      Exceptions.expectException(RollbackException.class, tm()::commit);
      assertEquals("Wrong final value for key hello", null, cache.get("hello"));
   }

   public void testSameNodeKeyCreation() throws Exception {
      tm().begin();
      assertEquals("Wrong value read by transaction 1 for key NewKey", null, cache.get("NewKey"));
      cache.put("NewKey", "v1");
      Transaction tx0 = tm().suspend();

      //other transaction do the same thing
      tm().begin();
      assertEquals("Wrong value read by transaction 2 for key NewKey", null, cache.get("NewKey"));
      cache.put("NewKey", "v2");
      tm().commit();

      tm().resume(tx0);
      Exceptions.expectException(RollbackException.class, tm()::commit);

      assertEquals("Wrong final value for key NewKey", "v2", cache.get("NewKey"));
   }
}
