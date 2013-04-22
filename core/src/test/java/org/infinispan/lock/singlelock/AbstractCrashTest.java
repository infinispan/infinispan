/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.lock.singlelock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.dld.ControlledRpcManager;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author Mircea Markus
 * @since 5.1
 */
public abstract class AbstractCrashTest extends MultipleCacheManagersTest {

   protected Configuration.CacheMode cacheMode;
   protected LockingMode lockingMode;
   protected Boolean useSynchronization;

   protected AbstractCrashTest(Configuration.CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      this.cacheMode = cacheMode;
      this.lockingMode = lockingMode;
      this.useSynchronization = useSynchronization;
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration c = buildConfiguration();
      createCluster(c, 3);
      waitForClusterToForm();
   }

   protected Configuration buildConfiguration() {
      final Configuration c = getDefaultClusteredConfig(cacheMode);
      c.fluent().
            transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .useSynchronization(useSynchronization)
            .lockingMode(lockingMode);
      c.fluent().hash()
            .rehashEnabled(false)
            .numOwners(3);
      c.fluent().clustering().l1().disable();
      return c;
   }


   protected Object beginAndPrepareTx(final Object k, final int cacheIndex) {
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(cacheIndex).begin();
               cache(cacheIndex).put(k,"v");
               final DummyTransaction transaction = (DummyTransaction) tm(cacheIndex).getTransaction();
               transaction.runPrepare();
            } catch (Throwable e) {
               e.printStackTrace();
            }
         }
      }, false);
      return k;
   }

   protected Object beginAndCommitTx(final Object k, final int cacheIndex) {
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(cacheIndex).begin();
               cache(cacheIndex).put(k, "v");
               tm(cacheIndex).commit();
            } catch (Throwable e) {
               log.errorf(e, "Error committing transaction for key %s on cache %s", k, cache(cacheIndex));
            }
         }
      }, false);
      return k;
   }

   public static class TxControlInterceptor extends CommandInterceptor {

      public CountDownLatch prepareProgress = new CountDownLatch(1);
      public CountDownLatch preparedReceived = new CountDownLatch(1);
      public CountDownLatch commitReceived = new CountDownLatch(1);
      public CountDownLatch commitProgress = new CountDownLatch(1);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         final Object result = super.visitPrepareCommand(ctx, command);
         preparedReceived.countDown();
         prepareProgress.await();
         return result;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         commitReceived.countDown();
         commitProgress.await();
         return super.visitCommitCommand(ctx, command);

      }
   }

   protected void prepareCache(final CountDownLatch releaseLocksLatch) {
      RpcManager rpcManager = new ControlledRpcManager(advancedCache(1).getRpcManager()) {
         @Override
         public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc,
                                                      RpcOptions options) {
            if (rpc instanceof TxCompletionNotificationCommand) {
               releaseLocksLatch.countDown();
               return null;
            } else {
               return realOne.invokeRemotely(recipients, rpc, options);
            }
         }
      };

      //not too nice: replace the rpc manager in the class that builds the Sync objects
      final TransactionTable transactionTable = TestingUtil.getTransactionTable(cache(1));
      TestingUtil.replaceField(rpcManager, "rpcManager", transactionTable, TransactionTable.class);

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      txControlInterceptor.prepareProgress.countDown();
      txControlInterceptor.commitProgress.countDown();
      advancedCache(1).addInterceptor(txControlInterceptor, 1);
   }

}
