package org.infinispan.tx.locking;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.Map;

import jakarta.transaction.SystemException;

import org.infinispan.commons.tx.XidImpl;
import org.infinispan.context.Flag;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractLocalTest extends SingleCacheManagerTest {

   public void testPut() throws Exception {
      tm().begin();
      cache.put("k", "v");
      assertLocking();
   }

   public void testRemove() throws Exception {
      tm().begin();
      cache.remove("k");
      assertLocking();
   }

   public void testReplace() throws Exception {
      cache.put("k", "initial");
      tm().begin();
      cache.replace("k", "v");
      assertLocking();
   }

   public void testPutAll() throws Exception {
      Map<String, String> m = Collections.singletonMap("k", "v");
      tm().begin();
      cache.putAll(m);
      assertLocking();
   }

   public void testRollback() throws Exception {
      tm().begin();
      cache().put("k", "v");
      assertLockingOnRollback();
      assertNull(cache().get("k"));
   }

   protected abstract void assertLockingOnRollback();

   protected abstract void assertLocking();

   protected void commit() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm();
      try {
         dtm.firstEnlistedResource().commit(getXid(), true);
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }

   protected void prepare() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm();
      try {
         dtm.firstEnlistedResource().prepare(getXid());
      } catch (Throwable e) {
         throw new RuntimeException(e);
      }
   }

   protected void rollback() {
      EmbeddedTransactionManager dtm = (EmbeddedTransactionManager) tm();
      try {
         dtm.getTransaction().rollback();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   private XidImpl getXid() throws SystemException {
      EmbeddedTransaction tx = (EmbeddedTransaction) tm().getTransaction();
      return tx.getXid();
   }

   public void testSizeAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertEquals(0, cache.size());
      } finally {
         tm().commit();
      }
   }

   public void testEntrySetIsEmptyAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertTrue(cache.entrySet().isEmpty());
      } finally {
         tm().commit();
      }
   }

   public void testEntrySetSizeAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertEquals(0, cache.entrySet().size());
      } finally {
         tm().commit();
      }
   }

   public void testKeySetIsEmptyAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertTrue(cache.keySet().isEmpty());
      } finally {
         tm().commit();
      }
   }

   public void testKeySetSizeAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertEquals(0, cache.keySet().size());
      } finally {
         tm().commit();
      }
   }

   public void testValuesIsEmptyAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertTrue(cache.values().isEmpty());
      } finally {
         tm().commit();
      }
   }

   public void testValuesSizeAfterLocalClear() throws Exception {
      cache.put(1, "v1");
      tm().begin();
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
         assertEquals(0, cache.values().size());
      } finally {
         tm().commit();
      }
   }
}
