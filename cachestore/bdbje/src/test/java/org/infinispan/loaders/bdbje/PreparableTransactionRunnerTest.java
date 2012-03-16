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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.*;

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
      config = createMock(EnvironmentConfig.class);
      expect(config.getTransactional()).andReturn(true);
      expect(config.getLocking()).andReturn(true);
      transaction = createMock(Transaction.class);
      env = createMock(Environment.class);
      expect(env.getConfig()).andReturn(config);
      expect(env.beginTransaction(null, null)).andReturn(transaction);
      worker = createMock(TransactionWorker.class);
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      runner = null;
      env = null;
      config = null;
   }


   @Test
   public void testMoreDeadlocks() throws Exception {
      Locker mockLocker = createNiceMock(Locker.class);
      worker.doWork();
      expectLastCall().andThrow(new LockTimeoutException(mockLocker, ""));
      transaction.abort();
      expect(env.beginTransaction(null, null)).andReturn(transaction);
      worker.doWork();
      expectLastCall().andThrow(new LockTimeoutException(mockLocker, ""));
      transaction.abort();
      expect(env.beginTransaction(null, null)).andReturn(transaction);
      worker.doWork();
      expectLastCall().andThrow(new LockTimeoutException(mockLocker, ""));
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env, 2, null);
      try {
         runner.prepare(worker);
         assert false : "should have gotten a deadlock exception";
      } catch (LockTimeoutException e) {

      }
      verifyAll();
   }

   @Test
   public void testPrepare() throws Exception {

      worker.doWork();
      replayAll();
      runner = new PreparableTransactionRunner(env);
      runner.prepare(worker);
      verifyAll();
   }

   @Test
   public void testRun() throws Exception {
      transaction.commit();
      worker.doWork();
      replayAll();
      runner = new PreparableTransactionRunner(env);
      runner.run(worker);
      verifyAll();
   }


   @Test
   public void testOneArgConstructorSetsCurrentTxn() throws Exception {
      replayAll();
      runner = new PreparableTransactionRunner(env);
      assert CurrentTransaction.getInstance(env) == runner.currentTxn;
   }

   @Test
   public void testSetMaxRetries() throws Exception {
      replayAll();
      runner = new PreparableTransactionRunner(env);
      runner.setMaxRetries(1);
      assert runner.getMaxRetries() == 1;
   }

   @Test
   public void testSetAllowNestedTransactions() throws Exception {
      replayAll();
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
      replayAll();
      TransactionConfig config = new TransactionConfig();
      runner = new PreparableTransactionRunner(env);
      runner.setTransactionConfig(config);
      assert runner.getTransactionConfig().equals(config);
   }


   @Test
   public void testExceptionThrownInPrepare() throws Exception {

      worker.doWork();
      expectLastCall().andThrow(new RuntimeException());
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (RuntimeException e) {

      }
      verifyAll();
   }

   @Test
   public void testErrorThrownInPrepare() throws Exception {

      worker.doWork();
      expectLastCall().andThrow(new Error());
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (Error e) {

      }
      verifyAll();
   }


   @Test
   public void testExceptionThrownInRun() throws Exception {

      worker.doWork();
      expectLastCall().andThrow(new RuntimeException());
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.prepare(worker);
         assert false : "should have gotten an exception";
      } catch (RuntimeException e) {

      }
      verifyAll();
   }

   @Test
   public void testErrorThrownInRun() throws Exception {

      worker.doWork();
      expectLastCall().andThrow(new Error());
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env);

      try {
         runner.run(worker);
         assert false : "should have gotten an exception";
      } catch (Error e) {

      }
      verifyAll();
   }


   public void testRethrowIfNotDeadLockDoesntThrowWhenGivenDeadlockException() throws Exception {
      replayAll();
      runner = new PreparableTransactionRunner(env);
      runner.rethrowIfNotDeadLock(createNiceMock(LockTimeoutException.class));
   }

   public void testThrowableDuringAbort() throws Exception {
      transaction.abort();
      expectLastCall().andThrow(new RuntimeException());
      replayAll();
      runner = new PreparableTransactionRunner(env);
      CurrentTransaction.getInstance(env).beginTransaction(null);
      int max = runner.abortOverflowingCurrentTriesOnError(transaction, 2);
      assert max == Integer.MAX_VALUE : "should have overflowed max tries, but got " + max;
      verifyAll();
   }

   private void replayAll() {
      replay(config);
      replay(env);
      replay(transaction);
      replay(worker);
   }

   private void verifyAll() {
      verify(config);
      verify(env);
      verify(transaction);
      verify(worker);
   }
}
