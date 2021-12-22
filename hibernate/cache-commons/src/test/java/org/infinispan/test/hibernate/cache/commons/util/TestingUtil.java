package org.infinispan.test.hibernate.cache.commons.util;

import java.util.concurrent.Callable;

import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;

public class TestingUtil {
   private TestingUtil() {
   }

   /**
    * Call an operation within a transaction. This method guarantees that the
    * right pattern is used to make sure that the transaction is always either
    * committed or rollbacked.
    *
    * @param tm transaction manager
    * @param c callable instance to run within a transaction
    * @param <T> type returned from the callable
    * @return returns whatever the callable returns
    */
   public static <T> T withTx(TransactionManager tm, Callable<T> c) throws Exception {
      return withTxCallable(tm, c).call();
   }

   /**
    * Returns a callable that will call the provided callable within a transaction.  This method guarantees that the
    * right pattern is used to make sure that the transaction is always either committed or rollbacked around
    * the callable.
    *
    * @param tm transaction manager
    * @param c callable instance to run within a transaction
    * @param <T> tyep of callable to return
    * @return The callable to invoke.  Note as long as the provided callable is thread safe this callable will be as well
    */
   public static <T> Callable<T> withTxCallable(final TransactionManager tm, final Callable<? extends T> c) {
      return () -> {
         tm.begin();
         try {
            return c.call();
         } catch (Exception e) {
            tm.setRollbackOnly();
            throw e;
         } finally {
            if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
            else tm.rollback();
         }
      };
   }
}
