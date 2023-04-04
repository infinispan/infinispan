package org.infinispan.tx;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

/**
 * Set of tests for the {@link org.infinispan.transaction.tm.EmbeddedTransaction}.
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
@Test(groups = "functional", testName = "tx.EmbeddedTransactionTest")
public class EmbeddedTransactionTest extends SingleCacheManagerTest {

   private static final String SYNC_CACHE_NAME = "sync-cache";
   private static final String XA_CACHE_NAME = "xa-cache";

   private static final String KEY = "key";
   private static final String VALUE = "value";

   public void testFailBeforeWithMarkRollbackFirstSync() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterFailSynchronization(FailMode.BEFORE_MARK_ROLLBACK),
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testFailBeforeWithExceptionFirstSync() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterFailSynchronization(FailMode.BEFORE_THROW_EXCEPTION),
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testFailBeforeWithMarkRollbackSecondSync() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailSynchronization(FailMode.BEFORE_MARK_ROLLBACK),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testFailBeforeWithExceptionSecondSync() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailSynchronization(FailMode.BEFORE_THROW_EXCEPTION),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testEndFailFirstXa() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_END),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testEndFailSecondXa() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_END)
      ));
   }

   public void testPrepareFailFirstXa() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_PREPARE),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testPrepareFailSecondXa() throws Exception {
      doCommitWithRollbackExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_PREPARE)
      ));
   }

   public void testCommitFailFirstXa() throws Exception {
      doCommitWithExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_COMMIT),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ), HeuristicMixedException.class, true);
   }

   public void testCommitFailSecondXa() throws Exception {
      doCommitWithExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_COMMIT)
      ), HeuristicMixedException.class, true);
   }

   public void testRollbackFailFirstXa() throws Exception {
      doRollbackWithHeuristicExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_ROLLBACK),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testRollbackFailSecondXa() throws Exception {
      doRollbackWithHeuristicExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_ROLLBACK)
      ));
   }

   public void testFailAfterFirstSync() throws Exception {
      doAfterCompletionFailTest(Arrays.asList(
            new RegisterFailSynchronization(FailMode.AFTER_THROW_EXCEPTION),
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testFailAfterSecondSync() throws Exception {
      doAfterCompletionFailTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterFailSynchronization(FailMode.AFTER_THROW_EXCEPTION),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME))
      ));
   }

   public void testReadOnlyResource() throws Exception {
      //test for ISPN-2813
      EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();
      transactionManager.begin();
      cacheManager.<String, String>getCache(SYNC_CACHE_NAME).put(KEY, VALUE);
      cacheManager.<String, String>getCache(XA_CACHE_NAME).put(KEY, VALUE);
      transactionManager.getTransaction().enlistResource(new ReadOnlyXaResource());
      transactionManager.commit();

      assertData();
      assertNoTxInAllCaches();
      assertNull(transactionManager.getTransaction());
   }

   public void testNoTransactionAtCommitAlone() throws Exception {
      doCommitWithExceptionTest(Collections.singletonList(new RegisterFailXaResource(FailMode.XA_COMMIT_WITH_NOTX)),
            HeuristicRollbackException.class, false);
   }

   public void testNoTransactionAtCommitWithOtherResources() throws Exception {
      doCommitWithExceptionTest(Arrays.asList(
            new RegisterCacheTransaction(cacheManager.getCache(SYNC_CACHE_NAME)),
            new RegisterCacheTransaction(cacheManager.getCache(XA_CACHE_NAME)),
            new RegisterFailXaResource(FailMode.XA_COMMIT_WITH_NOTX)
      ), HeuristicMixedException.class, true);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));

      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.transaction().useSynchronization(true);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheManager.defineConfiguration(SYNC_CACHE_NAME, builder.build());

      builder = getDefaultStandaloneCacheConfig(true);
      builder.transaction().useSynchronization(false);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheManager.defineConfiguration(XA_CACHE_NAME, builder.build());
      return cacheManager;
   }

   private void doCommitWithRollbackExceptionTest(Collection<RegisterTransaction> registerTransactionCollection) throws Exception {
      EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();
      transactionManager.begin();
      for (RegisterTransaction registerTransaction : registerTransactionCollection) {
         registerTransaction.register(transactionManager);
      }
      try {
         transactionManager.commit();
         fail("RollbackException expected!");
      } catch (RollbackException e) {
         //expected
      }

      assertEmpty();
      assertNoTxInAllCaches();
      assertNull(transactionManager.getTransaction());
   }

   private void doCommitWithExceptionTest(Collection<RegisterTransaction> registerTransactionCollection,
         Class<? extends Exception> exceptionClass, boolean checkData) throws Exception {
      EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();
      transactionManager.begin();
      for (RegisterTransaction registerTransaction : registerTransactionCollection) {
         registerTransaction.register(transactionManager);
      }
      expectException(exceptionClass, transactionManager::commit);

      if (checkData) {
         assertData();
      } else {
         assertEmpty();
      }
      assertNoTxInAllCaches();
      assertNull(transactionManager.getTransaction());
   }

   private void doRollbackWithHeuristicExceptionTest(Collection<RegisterTransaction> registerTransactionCollection) throws Exception {
      EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();
      transactionManager.begin();
      for (RegisterTransaction registerTransaction : registerTransactionCollection) {
         registerTransaction.register(transactionManager);
      }
      try {
         transactionManager.rollback();
         fail("SystemException expected!");
      } catch (SystemException e) {
         //expected
      }

      assertEmpty();
      assertNoTxInAllCaches();
      assertNull(transactionManager.getTransaction());
   }

   private void doAfterCompletionFailTest(Collection<RegisterTransaction> registerTransactionCollection) throws Exception {
      EmbeddedTransactionManager transactionManager = EmbeddedTransactionManager.getInstance();
      transactionManager.begin();
      for (RegisterTransaction registerTransaction : registerTransactionCollection) {
         registerTransaction.register(transactionManager);
      }
      transactionManager.commit();

      assertData();
      assertNoTxInAllCaches();
      assertNull(transactionManager.getTransaction());
   }

   private void assertEmpty() {
      assertTrue(cacheManager.getCache(SYNC_CACHE_NAME).isEmpty());
      assertTrue(cacheManager.getCache(XA_CACHE_NAME).isEmpty());
   }

   private void assertData() {
      assertEquals(VALUE, cacheManager.getCache(SYNC_CACHE_NAME).get(KEY));
      assertEquals(VALUE, cacheManager.getCache(XA_CACHE_NAME).get(KEY));
   }

   private void assertNoTxInAllCaches() {
      assertNoTransactions(cacheManager.getCache(XA_CACHE_NAME));
      assertNoTransactions(cacheManager.getCache(SYNC_CACHE_NAME));
   }

   private enum FailMode {
      BEFORE_MARK_ROLLBACK,
      BEFORE_THROW_EXCEPTION,
      AFTER_THROW_EXCEPTION,
      XA_PREPARE,
      XA_COMMIT,
      XA_COMMIT_WITH_NOTX,
      XA_ROLLBACK,
      XA_END
   }

   private interface RegisterTransaction {
      void register(TransactionManager transactionManager) throws Exception;
   }

   //a little hacky, but it is just to avoid implementing everything
   private static class ReadOnlyXaResource extends FailXaResource {

      private boolean finished;

      private ReadOnlyXaResource() {
         super(null);
      }

      @Override
      public int prepare(Xid xid) {
         finished = true;
         return XA_RDONLY;
      }

      @Override
      public void commit(Xid xid, boolean b) throws XAException {
         if (finished) {
            throw new XAException(XAException.XAER_NOTA);
         }
      }

      @Override
      public void rollback(Xid xid) throws XAException {
         if (finished) {
            throw new XAException(XAException.XAER_NOTA);
         }
      }
   }

   private static class FailXaResource implements XAResource {

      private final FailMode failMode;

      private FailXaResource(FailMode failMode) {
         this.failMode = failMode;
      }

      @Override
      public void commit(Xid xid, boolean b) throws XAException {
         switch (failMode) {
            case XA_COMMIT:
               throw new XAException(XAException.XA_HEURCOM);
            case XA_COMMIT_WITH_NOTX:
               throw new XAException(XAException.XAER_NOTA);
         }
      }

      @Override
      public void end(Xid xid, int i) throws XAException {
         if (failMode == FailMode.XA_END) {
            throw new XAException();
         }
      }

      @Override
      public void forget(Xid xid) {/*no-op*/}

      @Override
      public int getTransactionTimeout() {
         return 0;
      }

      @Override
      public boolean isSameRM(XAResource xaResource) {
         return xaResource instanceof FailSynchronization && ((FailSynchronization) xaResource).failMode == failMode;
      }

      @Override
      public int prepare(Xid xid) throws XAException {
         if (failMode == FailMode.XA_PREPARE) {
            throw new XAException();
         }
         return XA_OK;
      }

      @Override
      public Xid[] recover(int i) {
         return new Xid[0];
      }

      @Override
      public void rollback(Xid xid) throws XAException {
         if (failMode == FailMode.XA_ROLLBACK) {
            throw new XAException();
         }
      }

      @Override
      public boolean setTransactionTimeout(int i) {
         return false;
      }

      @Override
      public void start(Xid xid, int i) throws XAException {/*no-op*/}
   }

   private static class FailSynchronization implements Synchronization {

      private final Transaction transaction;
      private final FailMode failMode;

      private FailSynchronization(Transaction transaction, FailMode failMode) {
         this.transaction = transaction;
         this.failMode = failMode;
      }

      @Override
      public void beforeCompletion() {
         switch (failMode) {
            case BEFORE_MARK_ROLLBACK:
               try {
                  transaction.setRollbackOnly();
               } catch (SystemException e) {
                  /* ignored */
               }
               break;
            case BEFORE_THROW_EXCEPTION:
               throw new RuntimeException("induced!");
         }
      }

      @Override
      public void afterCompletion(int status) {
         if (failMode == FailMode.AFTER_THROW_EXCEPTION) {
            throw new RuntimeException("induced!");
         }
      }
   }

   private static class RegisterCacheTransaction implements RegisterTransaction {

      private final Cache<String, String> cache;

      private RegisterCacheTransaction(Cache<String, String> cache) {
         this.cache = cache;
      }

      @Override
      public void register(TransactionManager transactionManager) {
         cache.put(KEY, VALUE);
      }
   }

   private static class RegisterFailSynchronization implements RegisterTransaction {

      private final FailMode failMode;

      private RegisterFailSynchronization(FailMode failMode) {
         this.failMode = failMode;
      }

      @Override
      public void register(TransactionManager transactionManager) throws Exception {
         Transaction transaction = transactionManager.getTransaction();
         FailSynchronization failSynchronization = new FailSynchronization(transaction, failMode);
         transaction.registerSynchronization(failSynchronization);
      }
   }

   private static class RegisterFailXaResource implements RegisterTransaction {

      private final FailMode failMode;

      private RegisterFailXaResource(FailMode failMode) {
         this.failMode = failMode;
      }

      @Override
      public void register(TransactionManager transactionManager) throws Exception {
         Transaction transaction = transactionManager.getTransaction();
         transaction.enlistResource(new FailXaResource(failMode));
      }
   }

}
