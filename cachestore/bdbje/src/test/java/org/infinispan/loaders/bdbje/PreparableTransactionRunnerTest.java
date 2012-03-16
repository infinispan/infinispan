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
package org.infinispan.loaders.bdbje;

import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import static org.mockito.Mockito.*;

import com.sleepycat.je.txn.Locker;
import org.infinispan.loaders.CacheLoaderException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests that cover {@link  PreparableTransactionRunner }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.PreparableTransactionRunnerTest")
public class PreparableTransactionRunnerTest {
   PreparableTransactionRunner runner;
   Environment env;
   EnvironmentConfig config;
   TransactionWorker worker;
   Transaction transaction;

   @BeforeMethod
   public void setUp() throws Exception {
      config = mock(EnvironmentConfig.class);
      when(config.getTransactional()).thenReturn(true);
      when(config.getLocking()).thenReturn(true);
      transaction = mock(Transaction.class);
      env = mock(Environment.class);
      when(env.getConfig()).thenReturn(config);
      when(env.beginTransaction(null, null)).thenReturn(transaction);
      worker = mock(TransactionWorker.class);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      runner = null;
      env = null;
      config = null;
   }


   @Test
   public void testMoreDeadlocks() throws Exception {
      Locker mockLocker = mock(Locker.class);
      doThrow(new LockTimeoutException(mockLocker, "")).when(worker).doWork();
      when(env.beginTransaction(null, null)).thenReturn(transaction);
      runner = new PreparableTransactionRunner(env, 2, null);
      try {
         runner.prepare(worker);
         assert false : "should have gotten a deadlock exception";
      } catch (LockTimeoutException e) {

      }
   }

   @Test
   public void testPrepare() throws Exception {

      worker.doWork();
      runner = new PreparableTransactionRunner(env);
      runner.prepare(worker);
   }

   @Test
   public void testRun() throws Exception {
      transaction.commit();
      worker.doWork();
      runner = new PreparableTransactionRunner(env);
      runner.run(worker);
   }


   @Test
   public void testOneArgConstructorSetsCurrentTxn() throws Exception {
      runner = new PreparableTransactionRunner(env);
      assert CurrentTransaction.getInstance(env) == runner.currentTxn;
   }

   @Test
   public void testSetMaxRetries() throws Exception {
      runner = new PreparableTransactionRunner(env);
      runner.setMaxRetries(1);
      assert runner.getMaxRetries() == 1;
   }

   @Test
   public void testSetAllowNestedTransactions() throws Exception {
      runner = new PreparableTransactionRunner(env);
      runner.setAllowNestedTransactions(false);
      assert !runner.getAllowNestedTransactions();
      try {
         runner.setAllowNestedTransactions(true);
         assert false : "should have gotten Exception";
      } catch (UnsupportedOperationException e) {}
   }

   @Test
   public void testGetTransactionConfig() throws Exception {
      TransactionConfig config = new TransactionConfig();
      runner = new PreparableTransactionRunner(env);
      runner.setTransactionConfig(config);
      assert runner.getTransactionConfig().equals(config);
   }


   @Test
   public void testExceptionThrownInPrepare() throws Exception {
      doThrow(new RuntimeException()).when(worker).doWork();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (RuntimeException e) {

      }
   }

   @Test
   public void testErrorThrownInPrepare() throws Exception {
      doThrow(new Error()).when(worker).doWork();
      runner = new PreparableTransactionRunner(env);
      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (Error e) {

      }
   }


   @Test
   public void testExceptionThrownInRun() throws Exception {

      doThrow(new RuntimeException()).when(worker).doWork();
      runner = new PreparableTransactionRunner(env);
      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (RuntimeException e) {

      }
   }

   @Test
   public void testErrorThrownInRun() throws Exception {
      doThrow(new Error()).when(worker).doWork();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.run(worker);
         assert false : "should have gotten an exception";
      } catch (Error e) {

      }
   }


   public void testRethrowIfNotDeadLockDoesntThrowWhenGivenDeadlockException() throws Exception {
      runner = new PreparableTransactionRunner(env);
      runner.rethrowIfNotDeadLock(mock(LockTimeoutException.class));
   }

   public void testThrowableDuringAbort() throws Exception {
      doThrow(new RuntimeException()).when(transaction).abort();
      runner = new PreparableTransactionRunner(env);
      CurrentTransaction.getInstance(env).beginTransaction(null);
      int max = runner.abortOverflowingCurrentTriesOnError(transaction, 2);
      assert max == Integer.MAX_VALUE : "should have overflowed max tries, but got " + max;
   }
}
