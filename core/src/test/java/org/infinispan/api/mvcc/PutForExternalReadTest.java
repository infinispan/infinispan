package org.infinispan.api.mvcc;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestBlocking;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import jakarta.transaction.Status;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadTest")
@CleanupAfterMethod
public class PutForExternalReadTest extends MultipleCacheManagersTest {

   protected static final String CACHE_NAME = "pferSync";

   protected static final String key = "k", value = "v1", value2 = "v2";

   @Override
   public Object[] factory() {
      return new Object[] {
         new PutForExternalReadTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new PutForExternalReadTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new PutForExternalReadTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new PutForExternalReadTest().cacheMode(CacheMode.REPL_SYNC).transactional(false),
         new PutForExternalReadTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new PutForExternalReadTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
      };
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = createCacheConfigBuilder();
      createClusteredCaches(2, CACHE_NAME, TestDataSCI.INSTANCE, c);
   }

   protected ConfigurationBuilder createCacheConfigBuilder() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(cacheMode, transactional);
      c.clustering().hash().numOwners(100);
      c.clustering().hash().numSegments(4);
      if (lockingMode != null) {
         c.transaction().lockingMode(lockingMode);
      }
      return c;
   }

   // This test executes PFER on cache1, and expects that it will be relayed to cache2 == primary
   // and then sent to cache1 again for backup.
   @InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC})
   public void testKeyOnlyWrittenOnceOnOriginator() throws Exception {
      final Cache<MagicKey, String> cache1 = cache(0, CACHE_NAME);
      final Cache<MagicKey, String> cache2 = cache(1, CACHE_NAME);

      final CyclicBarrier barrier = new CyclicBarrier(2);
      cache1.getAdvancedCache().getAsyncInterceptorChain().addInterceptor(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command)
               throws Throwable {
            if (command instanceof PutKeyValueCommand) {
               if (!ctx.isOriginLocal()) {
                  // wait first before the check
                  TestBlocking.await(barrier, 10, TimeUnit.SECONDS);
                  // and once more after the check
                  TestBlocking.await(barrier, 10, TimeUnit.SECONDS);
               }
            }
            return invokeNext(ctx, command);
         }
      }, 0);

      final MagicKey myKey = new MagicKey(cache2);
      cache1.putForExternalRead(myKey, value);

      // Verify that the key was not written on the origin by the time it was looped back
      barrier.await(10, TimeUnit.SECONDS);
      assertNull(cache1.get(myKey));

      // Verify that the key is written on the origin afterwards
      barrier.await(10, TimeUnit.SECONDS);
      eventually(() -> value.equals(cache1.get(myKey)) && value.equals(cache2.get(myKey)));
   }

   public void testNoOpWhenKeyPresent() {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);
      cache1.putForExternalRead(key, value);

      eventually(() -> value.equals(cache1.get(key)) && value.equals(cache2.get(key)));

      // reset
      cache1.remove(key);

      eventually(() -> cache1.isEmpty() && cache2.isEmpty());

      cache1.put(key, value);

      eventually(() -> value.equals(cache1.get(key)) && value.equals(cache2.get(key)));

      // now this pfer should be a no-op
      cache1.putForExternalRead(key, value2);

      assertEquals("PFER should have been a no-op", value, cache1.get(key));
      assertEquals("PFER should have been a no-op", value, cache2.get(key));
   }

   @InTransactionMode(TransactionMode.TRANSACTIONAL)
   public void testTxSuspension() throws Exception {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);

      cache1.put(key + "0", value);

      eventually(() -> value.equals(cache2.get(key+"0")));

      // start a tx and do some stuff.
      tm(0, CACHE_NAME).begin();
      cache1.get(key + "0");
      cache1.putForExternalRead(key, value); // should have happened in a separate tx and have committed already.
      Transaction t = tm(0, CACHE_NAME).suspend();

      eventually(() -> value.equals(cache1.get(key)) && value.equals(cache2.get(key)));

      tm(0, CACHE_NAME).resume(t);
      tm(0, CACHE_NAME).commit();

      eventually(() -> value.equals(cache1.get(key + "0")) && value.equals(cache2.get(key + "0")));
   }

   public void testExceptionSuppression() throws Exception {
      Cache<String, String> cache1 = cache(0, CACHE_NAME);
      Cache<String, String> cache2 = cache(1, CACHE_NAME);

      assertTrue(cache1.getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(new BaseAsyncInterceptor() {
         @Override
         public Object visitCommand(InvocationContext ctx, VisitableCommand command)
               throws Throwable {
            if (command instanceof PutKeyValueCommand || command instanceof RemoveCommand) {
               throw new RuntimeException("Barf!");
            }
            return invokeNext(ctx, command);
         }
      }, CallInterceptor.class));

      // if cache1 is not primary, the value gets committed on cache2
      try {
         cache1.put(key, value);
         fail("Should have barfed");
      } catch (RuntimeException re) {
      }

      // clean up any indeterminate state left over
      try {
         cache1.remove(key);
         fail("Should have barfed");
      } catch (RuntimeException re) {
      }

      assertNull("Should have cleaned up", cache1.get(key));
      assertNull("Should have cleaned up", cache1.getAdvancedCache().getDataContainer().get(key));
      assertNull("Should have cleaned up", cache2.get(key));
      InternalCacheEntry<String, String> cache2Entry = cache2.getAdvancedCache().getDataContainer().get(key);
      assertTrue("Should have cleaned up", cache2Entry == null);

      // should not barf
      cache1.putForExternalRead(key, value);
   }

   public void testBasicPropagation() throws Exception {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);

      assertFalse(cache1.containsKey(key));
      assertFalse(cache2.containsKey(key));
      ReplListener replListener2 = replListener(cache2);

      replListener2.expect(PutKeyValueCommand.class);
      cache1.putForExternalRead(key, value);
      replListener2.waitForRpc();

      // wait for command the finish executing asynchronously
      eventually(() -> cache1.containsKey(key) && cache2.containsKey(key));

      assertEquals("PFER updated cache1", value, cache1.get(key));
      assertEquals("PFER propagated to cache2 as expected", value, cache2.get(key));

      // replication to cache 1 should NOT happen.
      cache2.putForExternalRead(key, value + "0");

      assertEquals("PFER updated cache2", value, cache2.get(key));
      assertEquals("Cache1 should be unaffected", value, cache1.get(key));
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    */
   public void testSimpleCacheModeLocal(Method m) throws Exception {
      cacheModeLocalTest(false, m);
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead() when the call
    * occurs inside a transaction.
    */
   @InTransactionMode(TransactionMode.TRANSACTIONAL)
   public void testCacheModeLocalInTx(Method m) throws Exception {
      cacheModeLocalTest(true, m);
   }

   /**
    * Tests that suspended transactions do not leak.  See JBCACHE-1246.
    */
   @InTransactionMode(TransactionMode.TRANSACTIONAL)
   public void testMemLeakOnSuspendedTransactions() throws Exception {
      Cache<String, String> cache1 = cache(0, CACHE_NAME);
      Cache<String, String> cache2 = cache(1, CACHE_NAME);
      TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      ReplListener replListener2 = replListener(cache2);

      replListener2.expect(PutKeyValueCommand.class);
      tm1.begin();
      cache1.putForExternalRead(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      final TransactionTable tt2 = TestingUtil.extractComponent(cache2, TransactionTable.class);

      eventually(() -> tt1.getRemoteTxCount() == 0 && tt1.getLocalTxCount() == 0 &&
            tt2.getRemoteTxCount() == 0 && tt2.getLocalTxCount() == 0);

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.putForExternalRead(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.put(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      log.info("Before commit!!");
      tm1.commit();

      eventually(() -> (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
            && (tt2.getLocalTxCount() == 0));

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      tm1.commit();

      eventually(() -> (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
            && (tt2.getLocalTxCount() == 0));

      replListener2.expectWithTx(PutKeyValueCommand.class, PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      cache1.put(key, value);
      tm1.commit();

      eventually(() -> (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
            && (tt2.getLocalTxCount() == 0));
   }

   public void testMultipleIdenticalPutForExternalReadCalls() {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);

      cache1.putForExternalRead(key, value);

      // wait for command the finish executing asynchronously
      eventually(() -> cache1.containsKey(key) && cache2.containsKey(key));

      cache1.putForExternalRead(key, value2);

      assertEquals(value, cache1.get(key));
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   private void cacheModeLocalTest(boolean transactional, Method m) throws Exception {
      Cache<String, String> cache1 = cache(0, CACHE_NAME);
      Cache<String, String> cache2 = cache(1, CACHE_NAME);
      TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      if (transactional)
         tm1.begin();

      String k = k(m);
      cache1.getAdvancedCache().withFlags(CACHE_MODE_LOCAL).putForExternalRead(k, v(m));
      assertTrue(cache1.getAdvancedCache().getDataContainer().containsKey(k));
      assertFalse(cache2.getAdvancedCache().withFlags(CACHE_MODE_LOCAL).containsKey(k));
      assertFalse(cache2.getAdvancedCache().getDataContainer().containsKey(k));

      if (transactional)
         tm1.commit();
   }
}
