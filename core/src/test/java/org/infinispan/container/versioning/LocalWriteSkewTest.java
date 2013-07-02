package org.infinispan.container.versioning;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

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
      Object v = cache.get("hello");
      assert "world 1".equals(v);
      Transaction t = tm().suspend();

      // Create a write skew
      cache.put("hello", "world 3");

      tm().resume(t);
      try {
         cache.put("hello", "world 2");
         assert false: "Should have detected write skew";
      } catch (CacheException e) {
         // expected
      }

      try {
         tm().commit();
         assert false: "Transaction should roll back";
      } catch (RollbackException re) {
         // expected
      }

      assert "world 3".equals(cache.get("hello"));
   }

   public void testWriteSkewMultiEntries() throws Exception {
      tm().begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      tm().commit();

      tm().begin();
      cache.put("k2", "v2000");
      Object v = cache.get("k1");
      assert "v1".equals(v);
      assert "v2000".equals(cache.get("k2"));
      Transaction t = tm().suspend();

      // Create a write skew
      // Auto-commit is true
      cache.put("k1", "v3");

      tm().resume(t);
      try {
         cache.put("k1", "v5000");
         assert false: "Should have detected write skew";
      } catch (CacheException e) {
         // expected
      }

      try {
         tm().commit();
         assert false: "Transaction should roll back";
      } catch (RollbackException re) {
         // expected
      }

      assert "v3".equals(cache.get("k1"));
      assert "v2".equals(cache.get("k2"));
   }

   public void testWriteSkewDisabled() throws Exception {
      cache = cacheManager.getCache("no-ws-chk");

      // Auto-commit is true

      cache.put("hello", "world 1");

      tm().begin();
      Object v = cache.get("hello");
      assert "world 1".equals(v);
      Transaction t = tm().suspend();

      // Create a write skew
      cache.put("hello", "world 3");

      tm().resume(t);
      cache.put("hello", "world 2");
      tm().commit();

      assert "world 2".equals(cache.get("hello"));
   }

   public void testNullEntries() throws Exception {
      // Auto-commit is true
      cache.put("hello", "world");

      tm().begin();
      assert "world".equals(cache.get("hello"));
      Transaction t = tm().suspend();

      cache.remove("hello");

      assert null == cache.get("hello");

      tm().resume(t);
      try {
         cache.put("hello", "world2");
         assert false: "Write skew should have been detected";
      } catch (CacheException expected) {
         // expected
      }

      try {
         tm().commit();
         assert false: "This transaction should roll back";
      } catch (RollbackException expected) {
         // expected
      }
      assert null == cache.get("hello");
   }

   public void testSameNodeKeyCreation() throws Exception {
      tm().begin();
      Assert.assertEquals(cache.get("NewKey"), null);
      cache.put("NewKey", "v1");
      Transaction tx0 = tm().suspend();

      //other transaction do the same thing
      tm().begin();
      Assert.assertEquals(cache.get("NewKey"), null);
      cache.put("NewKey", "v2");
      tm().commit();

      tm().resume(tx0);
      try {
         tm().commit();
         Assert.fail("The transaction should rollback");
      } catch (RollbackException expected) {
         //expected
      }

      Assert.assertEquals(cache.get("NewKey"), "v2");      
   }
}
