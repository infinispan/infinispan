/*
 *
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.NotSerializableException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.Serializable;

@Test(groups = "functional", testName = "replication.ReplicationExceptionTest")
public class ReplicationExceptionTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC,true);
      configuration.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      configuration.setLockAcquisitionTimeout(5000);
      createClusteredCaches(2, "syncReplCache", configuration);

      Configuration replAsync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true);
      defineConfigurationOnAllManagers("asyncReplCache", replAsync);

      Configuration replQueue = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true);
      replQueue.setUseReplQueue(true);
      defineConfigurationOnAllManagers("replQueueCache", replQueue);

      Configuration asyncMarshall = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true);
      asyncMarshall.setUseAsyncMarshalling(true);
      defineConfigurationOnAllManagers("asyncMarshallCache", asyncMarshall);
   }

   private TransactionManager beginTransaction() throws SystemException, NotSupportedException {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      return mgr;
   }

   public void testNonSerializableRepl() throws Exception {
      doNonSerializableReplTest("syncReplCache");
   }

   public void testNonSerializableAsyncRepl() throws Exception {
      doNonSerializableReplTest("asyncReplCache");
   }

   public void testNonSerializableReplQueue() throws Exception {
      doNonSerializableReplTest("replQueueCache");
   }

   public void testNonSerializableAsyncMarshalling() throws Exception {
      doNonSerializableReplTest("asyncMarshallCache");
   }

   private void doNonSerializableReplTest(String cacheName) {
      AdvancedCache cache1 = cache(0, cacheName).getAdvancedCache();
      AdvancedCache cache2 = cache(1, cacheName).getAdvancedCache();
      try {
         cache1.put("test", new ContainerData());
         // We should not come here.
         assertNotNull("NonSerializableData should not be null on cache2", cache2.get("test"));
      } catch (RuntimeException runtime) {
         Throwable t = runtime.getCause();
         if (runtime instanceof NotSerializableException
                  || t instanceof NotSerializableException
                  || t.getCause() instanceof NotSerializableException) {
            System.out.println("received NotSerializableException - as expected");
         } else {
            throw runtime;
         }
      }
   }

   public void testNonSerializableReplWithTx() throws Exception {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();
      AdvancedCache cache2 = cache(1, "syncReplCache").getAdvancedCache();
      TransactionManager tm;

      try {
         tm = beginTransaction();
         cache1.put("test", new ContainerData());
         tm.commit();

         // We should not come here.
         assertNotNull("NonSerializableData should not be null on cache2", cache2.get("test"));
      } catch (RollbackException rollback) {
         System.out.println("received RollbackException - as expected");
      } catch (Exception e) {
         // We should also examine that it is indeed throwing a NonSerilaizable exception.
         fail(e.toString());
      }
   }

   @Test(groups = "functional", expectedExceptions = { TimeoutException.class })
   public void testSyncReplTimeout() {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();
      AdvancedCache cache2 = cache(1, "syncReplCache").getAdvancedCache();
      cache2.addInterceptor(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand cmd)
                  throws Throwable {
            // Add a delay
            Thread.sleep(100);
            return super.handleDefault(ctx, cmd);
         }
      }, 0);

      cache1.getConfiguration().setSyncReplTimeout(1);
      cache2.getConfiguration().setSyncReplTimeout(1);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      cache1.put("k", "v");
   }

   @Test(groups = "functional", expectedExceptions = { TimeoutException.class })
   public void testLockAcquisitionTimeout() throws Exception {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();
      AdvancedCache cache2 = cache(1, "syncReplCache").getAdvancedCache();
      cache2.getConfiguration().setLockAcquisitionTimeout(1);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      // get a lock on cache 2 and hold on to it.
      TransactionManager tm = TestingUtil.getTransactionManager(cache2);
      tm.begin();
      cache2.put("block", "block");
      tm.suspend();
      cache1.put("block", "v");
   }

   static class NonSerializabeData {
      int i;
   }

   public static class ContainerData implements Serializable {
      int i;
      NonSerializabeData non_serializable_data;
      private static final long serialVersionUID = -8322197791060897247L;

      public ContainerData() {
         i = 99;
         non_serializable_data = new NonSerializabeData();
      }
   }
}
