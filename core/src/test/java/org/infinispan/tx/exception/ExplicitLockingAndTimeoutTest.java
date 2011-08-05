/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.tx.exception;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.timeout.ExplicitLockingAndTimeoutTest")
public class ExplicitLockingAndTimeoutTest extends MultipleCacheManagersTest {

   private LockManager lm1;
   private LockManager lm0;
   private TransactionTable txTable0;
   private TransactionTable txTable1;
   private TransactionManager tm;
   private TxStatusInterceptor txStatus = new TxStatusInterceptor();

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration defaultConfig = getDefaultConfig();
      defaultConfig.setLockAcquisitionTimeout(500);
      defaultConfig.setUseLockStriping(false);
      addClusterEnabledCacheManager(defaultConfig);
      addClusterEnabledCacheManager(defaultConfig);
      lm0 = TestingUtil.extractLockManager(cache(0));
      lm1 = TestingUtil.extractLockManager(cache(1));
      txTable0 = TestingUtil.getTransactionTable(cache(0));
      txTable1 = TestingUtil.getTransactionTable(cache(1));
      tm = cache(0).getAdvancedCache().getTransactionManager();
      cache(1).getAdvancedCache().addInterceptor(txStatus, 0);
      TestingUtil.blockUntilViewReceived(cache(0), 2, 10000);
   }

   protected Configuration getDefaultConfig() {
      return getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
   }

   public void testExplicitLockingRemoteTimeout() throws NotSupportedException, SystemException, HeuristicMixedException, HeuristicRollbackException, InvalidTransactionException, RollbackException {
      txStatus.reset();
      tm.begin();
      cache(1).put("k1", "v1");
      Transaction k1LockOwner = tm.suspend();
      assert lm1.isLocked("k1");

      assertEquals(1, txTable1.getLocalTxCount());
      tm.begin();
      cache(0).getAdvancedCache().lock("k2");
      assert lm0.isLocked("k2");
      assert lm1.isLocked("k2");

      try {
         cache(0).getAdvancedCache().lock("k1");
         assert false;
      } catch (TimeoutException e) {
         //expected
      }

      assert txStatus.teReceived;
      assert txStatus.isTxInTableAfterTeOnEagerLocking;
      //expect 1 as k1 is locked by the other tx
      assertEquals(lm1.isLocked("k2"), false, "Even though rollback was not received yet lock on k2, which was acquired, is no longer held");
      assert tm.getStatus() == Status.STATUS_MARKED_ROLLBACK;

      assertEquals(1, txTable0.getLocalTxCount());
      assertEquals(1, txTable1.getLocalTxCount());
      assertEquals(1, txTable1.getRemoteTxCount());

      tm.rollback();
      assertEquals(0, txTable0.getLocalTxCount());
      assertEquals(1, txTable1.getLocalTxCount());
      assertEquals(1, txTable1.getRemoteTxCount());


      tm.resume(k1LockOwner);
      tm.commit();
      assertEquals("v1", cache(0).get("k1"));
      assertEquals("v1", cache(1).get("k1"));
      assertEquals(0, txTable1.getLocalTxCount());
      assertEquals(0, txTable0.getLocalTxCount());
      assertEquals(0, lm0.getNumberOfLocksHeld());
      assertEquals(0, lm1.getNumberOfLocksHeld());
   }

   private class TxStatusInterceptor extends CommandInterceptor {

      private boolean teReceived;

      private boolean isTxInTableAfterTeOnEagerLocking;

      private int numLocksAfterTeOnEagerLocking;

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } catch (TimeoutException te) {
            teReceived = true;
            isTxInTableAfterTeOnEagerLocking = txTable1.containRemoteTx(ctx.getGlobalTransaction());
            numLocksAfterTeOnEagerLocking = lm1.getNumberOfLocksHeld();
            throw te;
         }
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         return super.handleDefault(ctx, command);
      }

      public void reset() {
         this.teReceived = false;
         this.isTxInTableAfterTeOnEagerLocking = false;
         this.numLocksAfterTeOnEagerLocking = -1;
      }
   }
}