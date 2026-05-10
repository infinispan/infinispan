package org.infinispan.container.versioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.Flag;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;

@Test(testName = "container.versioning.ReplWriteSkewTest", groups = "functional")
@CleanupAfterMethod
public class ReplWriteSkewTest extends AbstractClusteredWriteSkewTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Override
   protected int clusterSize() {
      return 2;
   }

   public void testWriteSkew() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      // Auto-commit is true
      cache0.put("hello", "world 1");

      tm(0).begin();
      assertEquals("world 1", cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Induce a write skew
      cache1.put("hello", "world 3");

      assertEquals("world 3", cache0.get("hello"));
      assertEquals("world 3", cache1.get("hello"));

      tm(0).resume(t);
      cache0.put("hello", "world 2");

      try {
         tm(0).commit();
         fail("Transaction should roll back");
      } catch (RollbackException | HeuristicRollbackException re) {
         // expected
      }

      assertEquals("world 3", cache0.get("hello"));
      assertEquals("world 3", cache1.get("hello"));
   }

   public void testWriteSkewMultiEntries() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      tm(0).begin();
      cache0.put("hello", "world 1");
      cache0.put("hello2", "world 1");
      tm(0).commit();

      tm(0).begin();
      cache0.put("hello2", "world 2");
      assertEquals("world 2", cache0.get("hello2"));
      assertEquals("world 1", cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Induce a write skew
      // Auto-commit is true
      cache1.put("hello", "world 3");

      assertEquals("world 3", cache0.get("hello"));
      assertEquals("world 1", cache0.get("hello2"));
      assertEquals("world 3", cache1.get("hello"));
      assertEquals("world 1", cache1.get("hello2"));

      tm(0).resume(t);
      cache0.put("hello", "world 2");

      try {
         tm(0).commit();
         fail("Transaction should roll back");
      } catch (RollbackException|HeuristicRollbackException re) {
         // expected
      }

      assertEquals("world 3", cache0.get("hello"));
      assertEquals("world 1", cache0.get("hello2"));
      assertEquals("world 3", cache1.get("hello"));
      assertEquals("world 1", cache1.get("hello2"));
   }

   public void testNullEntries() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      // Auto-commit is true
      cache0.put("hello", "world");

      tm(0).begin();
      assertEquals("world", cache0.get("hello"));
      Transaction t = tm(0).suspend();

      cache1.remove("hello");

      assertNull(cache0.get("hello"));
      assertNull(cache1.get("hello"));

      tm(0).resume(t);
      cache0.put("hello", "world2");

      try {
         tm(0).commit();
         fail("This transaction should roll back");
      } catch (RollbackException|HeuristicRollbackException expected) {
         // expected
      }

      assertNull(cache0.get("hello"));
      assertNull(cache1.get("hello"));
   }

   public void testResendPrepare() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      // Auto-commit is true
      cache0.put("hello", "world");

      // create a write skew
      tm(0).begin();
      assertEquals("world", cache0.get("hello"));
      Transaction t = tm(0).suspend();

      // Implicit tx.  Prepare should be retried.
      cache(0).put("hello", "world2");

      assertEquals("world2", cache0.get("hello"));
      assertEquals("world2", cache1.get("hello"));

      tm(0).resume(t);
      cache0.put("hello", "world3");

      try {
         log.warn("----- Now committing ---- ");
         tm(0).commit();
         fail("This transaction should roll back");
      } catch (RollbackException|HeuristicRollbackException expected) {
         // expected
      }

      assertEquals("world2", cache0.get("hello"));
      assertEquals("world2", cache1.get("hello"));
   }

   public void testLocalOnlyPut() {
      localOnlyPut(this.<Integer, String>cache(0), 1, "v1");
      localOnlyPut(this.<Integer, String>cache(1), 2, "v2");
   }

   public void testSameNodeKeyCreation() throws Exception {
      tm(0).begin();
      assertNull(cache(0).get("NewKey"));
      cache(0).put("NewKey", "v1");
      Transaction tx0 = tm(0).suspend();

      //other transaction do the same thing
      tm(0).begin();
      assertNull(cache(0).get("NewKey"));
      cache(0).put("NewKey", "v2");
      tm(0).commit();

      tm(0).resume(tx0);
      try {
         tm(0).commit();
         fail("The transaction should rollback");
      } catch (RollbackException|HeuristicRollbackException expected) {
         //expected
      }

      assertEquals("v2", cache(0).get("NewKey"));
      assertEquals("v2", cache(1).get("NewKey"));
   }

   public void testDifferentNodeKeyCreation() throws Exception {
      tm(0).begin();
      assertNull(cache(0).get("NewKey"));
      cache(0).put("NewKey", "v1");
      Transaction tx0 = tm(0).suspend();

      //other transaction, in other node,  do the same thing
      tm(1).begin();
      assertNull(cache(1).get("NewKey"));
      cache(1).put("NewKey", "v2");
      tm(1).commit();

      tm(0).resume(tx0);
      try {
         tm(0).commit();
         fail("The transaction should rollback");
      } catch (RollbackException|HeuristicRollbackException expected) {
         //expected
      }

      assertEquals("v2", cache(0).get("NewKey"));
      assertEquals("v2", cache(1).get("NewKey"));
   }

   private void localOnlyPut(Cache<Integer, String> cache, Integer k, String v) {
      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put(k, v);
   }

}
