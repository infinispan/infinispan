/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

/*
 *
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.infinispan.replication;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.NotSerializableException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import java.io.Serializable;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "replication.ReplicationExceptionTest")
public class ReplicationExceptionTest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC,true)
            .fluent()
               .locking()
                  .isolationLevel(IsolationLevel.REPEATABLE_READ)
                  .lockAcquisitionTimeout(60000l)
               .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .build();
      createClusteredCaches(2, "syncReplCache", configuration);
      waitForClusterToForm("syncReplCache");

      Configuration noTx = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC,false)
            .fluent()
               .locking()
                  .isolationLevel(IsolationLevel.REPEATABLE_READ)
                  .lockAcquisitionTimeout(60000l).build();

      defineConfigurationOnAllManagers("syncReplCacheNoTx", noTx);

      Configuration replAsync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true);
      defineConfigurationOnAllManagers("asyncReplCache", replAsync);

      Configuration replAsyncNoTx = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,false);
      defineConfigurationOnAllManagers("asyncReplCacheNoTx", replAsyncNoTx);

      Configuration replQueue = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true)
            .fluent()
               .clustering()
                  .async()
                     .useReplQueue(true)
               .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .build();
      defineConfigurationOnAllManagers("replQueueCache", replQueue);

      Configuration asyncMarshall = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC,true)
            .fluent()
               .clustering()
                  .async()
                     .asyncMarshalling(true)
               .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .build();
      defineConfigurationOnAllManagers("asyncMarshallCache", asyncMarshall);
   }

   private TransactionManager beginTransaction() throws SystemException, NotSupportedException {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      return mgr;
   }

   public void testNonSerializableRepl() throws Exception {
      doNonSerializableReplTest("syncReplCacheNoTx");
   }

   public void testNonSerializableAsyncRepl() throws Exception {
      doNonSerializableReplTest("asyncReplCacheNoTx");
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

   @Test(groups = "functional", expectedExceptions = { CacheException.class })
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

      cache1.getConfiguration().setSyncReplTimeout(10);
      cache2.getConfiguration().setSyncReplTimeout(10);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      cache1.put("k", "v");
   }

   @Test(groups = "functional", expectedExceptions = { CacheException.class })
   public void testLockAcquisitionTimeout() throws Exception {
      AdvancedCache cache1 = cache(0, "syncReplCache").getAdvancedCache();
      AdvancedCache cache2 = cache(1, "syncReplCache").getAdvancedCache();
      cache1.getConfiguration().setLockAcquisitionTimeout(10);
      cache2.getConfiguration().setLockAcquisitionTimeout(10);
      TestingUtil.blockUntilViewsReceived(10000, cache1, cache2);

      // get a lock on cache 2 and hold on to it.
      DummyTransactionManager tm = (DummyTransactionManager) TestingUtil.getTransactionManager(cache2);
      tm.begin();
      cache2.put("block", "block");
      assert tm.getTransaction().runPrepare();
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
