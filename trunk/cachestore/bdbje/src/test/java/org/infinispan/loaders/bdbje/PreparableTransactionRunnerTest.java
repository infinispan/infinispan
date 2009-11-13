package org.infinispan.loaders.bdbje;

import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.collections.TransactionWorker;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.classextension.EasyMock.*;
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
      worker.doWork();
      expectLastCall().andThrow(new DeadlockException());
      transaction.abort();
      expect(env.beginTransaction(null, null)).andReturn(transaction);
      worker.doWork();
      expectLastCall().andThrow(new DeadlockException());
      transaction.abort();
      expect(env.beginTransaction(null, null)).andReturn(transaction);
      worker.doWork();
      expectLastCall().andThrow(new DeadlockException());
      transaction.abort();
      replayAll();
      runner = new PreparableTransactionRunner(env, 2, null);
      try {
         runner.prepare(worker);
         assert false : "should have gotten a deadlock exception";
      } catch (DeadlockException e) {

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
      runner.rethrowIfNotDeadLock(createNiceMock(DeadlockException.class));
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
