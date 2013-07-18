package org.infinispan.container.versioning;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import static org.testng.AssertJUnit.*;

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
      builder
            .versioning()
               .enable()
               .scheme(VersioningScheme.SIMPLE)
            .locking()
               .isolationLevel(IsolationLevel.REPEATABLE_READ)
               .writeSkewCheck(true)
            .transaction()
               .lockingMode(LockingMode.OPTIMISTIC);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      builder.locking().writeSkewCheck(false).versioning().disable();
      cm.defineConfiguration("no-ws-chk", builder.build());
      return cm;
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

      try {
         tm().commit();
         fail("Transaction should roll back");
      } catch (RollbackException re) {
         // expected
      }

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

      try {
         tm().commit();
         fail("Transaction should roll back");
      } catch (RollbackException re) {
         // expected
      }

      assertEquals("Wrong final value for key k1", "v3", cache.get("k1"));
      assertEquals("Wrong final value for key k2", "v2", cache.get("k2"));
   }

   public void testWriteSkewDisabled() throws Exception {
      cache = cacheManager.getCache("no-ws-chk");

      // Auto-commit is true

      cache.put("hello", "world 1");

      tm().begin();
      assertEquals("Wrong value read by transaction for key hello", "world 1", cache.get("hello"));
      Transaction t = tm().suspend();

      // Create a write skew
      cache.put("hello", "world 3");

      tm().resume(t);
      cache.put("hello", "world 2");
      tm().commit();

      assertEquals("Wrong final value for key hello", "world 2", cache.get("hello"));
   }

   public void testNullEntries() throws Exception {
      // Auto-commit is true
      cache.put("hello", "world");

      tm().begin();
      assertEquals("Wrong value read by transaction for key hello", "world", cache.get("hello"));
      Transaction t = tm().suspend();

      cache.remove("hello");

      assertNull("Wrong value after remove for key hello", cache.get("hello"));

      tm().resume(t);
      cache.put("hello", "world2");

      try {
         tm().commit();
         fail("Transaction should roll back");
      } catch (RollbackException expected) {
         // expected
      }
      assertNull("Wrong final value for key hello", cache.get("hello"));
   }

   public void testSameNodeKeyCreation() throws Exception {
      tm().begin();
      assertNull("Wrong value read by transaction 1 for key NewKey", cache.get("NewKey"));
      cache.put("NewKey", "v1");
      Transaction tx0 = tm().suspend();

      //other transaction do the same thing
      tm().begin();
      assertNull("Wrong value read by transaction 2 for key NewKey", cache.get("NewKey"));
      cache.put("NewKey", "v2");
      tm().commit();

      tm().resume(tx0);
      try {
         tm().commit();
         fail("The transaction should rollback");
      } catch (RollbackException expected) {
         //expected
      }

      assertEquals("Wrong final value for key NewKey", "v2", cache.get("NewKey"));
   }
}
