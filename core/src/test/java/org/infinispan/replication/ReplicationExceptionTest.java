package org.infinispan.replication;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.Serializable;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.ReplicationExceptionTest")
public class ReplicationExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configuration.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .lockAcquisitionTimeout(60000L)
            .transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      createClusteredCaches(2, "syncReplCache", configuration);
      waitForClusterToForm("syncReplCache");

      ConfigurationBuilder noTx = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      noTx.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .lockAcquisitionTimeout(60000L);
      defineConfigurationOnAllManagers("syncReplCacheNoTx", noTx);

      ConfigurationBuilder replAsyncNoTx = getDefaultClusteredCacheConfig(CacheMode.REPL_ASYNC, false);
      defineConfigurationOnAllManagers("asyncReplCacheNoTx", replAsyncNoTx);
   }

   private TransactionManager beginTransaction() throws SystemException, NotSupportedException {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      return mgr;
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonMarshallableRepl() {
      doNonSerializableReplTest("syncReplCacheNoTx");
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonMarshallableAsyncRepl() {
      doNonSerializableReplTest("asyncReplCacheNoTx");
   }

   private void doNonSerializableReplTest(String cacheName) {
      AdvancedCache<Object, Object> cache1 = advancedCache(0, cacheName);
      AdvancedCache<Object, Object> cache2 = advancedCache(1, cacheName);
      cache1.put("test", new ContainerData());
   }

   public void testNonSerializableReplWithTx() throws Exception {
      AdvancedCache<Object, Object> cache1 = advancedCache(0, "syncReplCache");
      AdvancedCache<Object, Object> cache2 = advancedCache(1, "syncReplCache");
      TransactionManager tm;

      try {
         tm = beginTransaction();
         cache1.put("test", new ContainerData());
         tm.commit();

         // We should not come here.
         assertNotNull("NonSerializableData should not be null on cache2", cache2.get("test"));
      } catch (RollbackException rollback) {
         log.trace("received RollbackException - as expected");
      } catch (Exception e) {
         // We should also examine that it is indeed throwing a NonSerilaizable exception.
         fail(e.toString());
      }
   }

   @Test(groups = "functional", expectedExceptions = { CacheException.class })
   public void testSyncReplTimeout() {
      AdvancedCache<Object, Object> cache1 = advancedCache(0, "syncReplCache");
      AdvancedCache<Object, Object> cache2 = advancedCache(1, "syncReplCache");
      extractInterceptorChain(cache2).addInterceptor(new DelayInterceptor(), 0);

      cache1.getCacheConfiguration().clustering().remoteTimeout(10);
      cache2.getCacheConfiguration().clustering().remoteTimeout(10);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      cache1.put("k", "v");
   }

   @Test(groups = "functional", expectedExceptions = { CacheException.class })
   public void testLockAcquisitionTimeout() throws Exception {
      AdvancedCache<Object, Object> cache1 = advancedCache(0, "syncReplCache");
      AdvancedCache<Object, Object> cache2 = advancedCache(1, "syncReplCache");
      cache1.getCacheConfiguration().locking().lockAcquisitionTimeout(10);
      cache2.getCacheConfiguration().locking().lockAcquisitionTimeout(10);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      // get a lock on cache 2 and hold on to it.
      EmbeddedTransactionManager tm = (EmbeddedTransactionManager) TestingUtil.getTransactionManager(cache2);
      tm.begin();
      cache2.put("block", "block");
      assertTrue(tm.getTransaction().runPrepare());
      tm.suspend();
      cache1.put("block", "v");
   }

   static class NonSerializabeData {
      int i;
   }

   public static class ContainerData implements Serializable, ExternalPojo {
      int i;
      NonSerializabeData non_serializable_data;
      private static final long serialVersionUID = -8322197791060897247L;

      public ContainerData() {
         i = 99;
         non_serializable_data = new NonSerializabeData();
      }
   }

   static class DelayInterceptor extends BaseAsyncInterceptor {
      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         // Add a delay
         Thread.sleep(100);
         return invokeNext(ctx, command);
      }
   }
}
