package org.infinispan.api.mvcc;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

@Test(groups = "functional")
@CleanupAfterMethod
public abstract class PutForExternalReadTest extends MultipleCacheManagersTest {

   protected static final String CACHE_NAME = "pferSync";

   protected static final String key = "k", value = "v1", value2 = "v2";

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder c = createCacheConfigBuilder();
      createClusteredCaches(2, CACHE_NAME, c);
   }

   protected abstract ConfigurationBuilder createCacheConfigBuilder();

   public void testKeyOnlyWrittenOnceOnOriginator() throws Exception {
      final Cache<MagicKey, String> cache1 = cache(0, CACHE_NAME);
      final Cache<MagicKey, String> cache2 = cache(1, CACHE_NAME);

      final CyclicBarrier barrier = new CyclicBarrier(2);
      cache1.getAdvancedCache().addInterceptor(new BaseCustomInterceptor() {
         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            if (!ctx.isOriginLocal()) {
               // wait first before the check
               barrier.await(10, TimeUnit.SECONDS);
               // and once more after the check
               barrier.await(10, TimeUnit.SECONDS);
            }

            return invokeNextInterceptor(ctx, command);
         }
      }, 0);

      final MagicKey myKey = new MagicKey(cache2);
      cache1.putForExternalRead(myKey, value);

      // Verify that the key was not written on the origin by the time it was looped back
      barrier.await(10, TimeUnit.SECONDS);
      assertNull(cache1.get(myKey));

      // Verify that the key is written on the origin afterwards
      barrier.await(10, TimeUnit.SECONDS);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(myKey)) && value.equals(cache2.get(myKey));
         }
      });
   }

   public void testNoOpWhenKeyPresent() {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);
      cache1.putForExternalRead(key, value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      // reset
      cache1.remove(key);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.isEmpty() && cache2.isEmpty();
         }
      });

      cache1.put(key, value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      // now this pfer should be a no-op
      cache1.putForExternalRead(key, value2);

      assertEquals("PFER should have been a no-op", value, cache1.get(key));
      assertEquals("PFER should have been a no-op", value, cache2.get(key));
   }

   public void testTxSuspension() throws Exception {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);

      cache1.put(key + "0", value);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache2.get(key+"0"));
         }
      });

      // start a tx and do some stuff.
      tm(0, CACHE_NAME).begin();
      cache1.get(key + "0");
      cache1.putForExternalRead(key, value); // should have happened in a separate tx and have committed already.
      Transaction t = tm(0, CACHE_NAME).suspend();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key)) && value.equals(cache2.get(key));
         }
      });

      tm(0, CACHE_NAME).resume(t);
      tm(0, CACHE_NAME).commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return value.equals(cache1.get(key + "0")) && value.equals(cache2.get(key + "0"));
         }
      });
   }

   public void testExceptionSuppression() throws Exception {
      Cache<String, String> cache1 = cache(0, CACHE_NAME);
      Cache<String, String> cache2 = cache(1, CACHE_NAME);

      cache1.getAdvancedCache().addInterceptorBefore(new CommandInterceptor() {
         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            throw new RuntimeException("Barf!");
         }

         @Override
         public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
            throw new RuntimeException("Barf!");
         }
      }, CallInterceptor.class);

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
      assertNull("Should have cleaned up", cache2.getAdvancedCache().getDataContainer().get(key));

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
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.containsKey(key) && cache2.containsKey(key);
         }
      });

      assertEquals("PFER updated cache1", value, cache1.get(key));
      assertEquals("PFER propagated to cache2 as expected", value, cache2.get(key));

      // replication to cache 1 should NOT happen.
      cache2.putForExternalRead(key, value + "0");

      assertEquals("PFER updated cache2", value, cache2.get(key));
      assertEquals("Cache1 should be unaffected", value, cache1.get(key));
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead().
    *
    * @throws Exception
    */
   public void testSimpleCacheModeLocal(Method m) throws Exception {
      cacheModeLocalTest(false, m);
   }

   /**
    * Tests that setting a cacheModeLocal=true flag prevents propagation of the putForExternalRead() when the call
    * occurs inside a transaction.
    *
    * @throws Exception
    */
   public void testCacheModeLocalInTx(Method m) throws Exception {
      cacheModeLocalTest(true, m);
   }

   /**
    * Tests that suspended transactions do not leak.  See JBCACHE-1246.
    */
   public void testMemLeakOnSuspendedTransactions() throws Exception {
      Cache<String, String> cache1 = cache(0, CACHE_NAME);
      Cache<String, String> cache2 = cache(1, CACHE_NAME);
      TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      TransactionManager tm2 = TestingUtil.getTransactionManager(cache2);
      ReplListener replListener2 = replListener(cache2);

      replListener2.expect(PutKeyValueCommand.class);
      tm1.begin();
      cache1.putForExternalRead(key, value);
      tm1.commit();
      replListener2.waitForRpc();

      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      final TransactionTable tt2 = TestingUtil.extractComponent(cache2, TransactionTable.class);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt1.getRemoteTxCount() == 0 && tt1.getLocalTxCount() == 0 &&
                  tt2.getRemoteTxCount() == 0 && tt2.getLocalTxCount() == 0;
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.putForExternalRead(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      cache1.put(key, value);
      assertEquals(tm1.getTransaction().getStatus(), Status.STATUS_ACTIVE);
      log.info("Before commit!!");
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });

      replListener2.expectWithTx(PutKeyValueCommand.class, PutKeyValueCommand.class);
      tm1.begin();
      cache1.put(key, value);
      cache1.putForExternalRead(key, value);
      cache1.put(key, value);
      tm1.commit();

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return (tt1.getRemoteTxCount() == 0) && (tt1.getLocalTxCount() == 0) &&  (tt2.getRemoteTxCount() == 0)
                  && (tt2.getLocalTxCount() == 0);
         }
      });
   }

   public void testMultipleIdenticalPutForExternalReadCalls() {
      final Cache<String, String> cache1 = cache(0, CACHE_NAME);
      final Cache<String, String> cache2 = cache(1, CACHE_NAME);

      cache1.putForExternalRead(key, value);

      // wait for command the finish executing asynchronously
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache1.containsKey(key) && cache2.containsKey(key);
         }
      });

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
      assertFalse(cache2.containsKey(k));

      if (transactional)
         tm1.commit();
   }
}
