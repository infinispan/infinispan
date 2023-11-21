package org.infinispan.test.hibernate.cache.commons.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.testing.env.ConnectionProviderBuilder;

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

   public static ConnectionProvider buildConnectionProvider() {
      try {
         Method method = ConnectionProviderBuilder.class.getMethod("buildConnectionProvider");
         return (ConnectionProvider) method.invoke(null);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
         throw new RuntimeException(e);
      }
   }

   public static ConnectionProvider buildConnectionProvider(boolean allowAggressiveRelease) {
      try {
         Method method = ConnectionProviderBuilder.class.getMethod("buildConnectionProvider", boolean.class);
         return (ConnectionProvider) method.invoke(null, allowAggressiveRelease);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
         throw new RuntimeException(e);
      }
   }

   public static ConnectionProvider buildConnectionProvider(String dbName) {
      try {
         Method method = ConnectionProviderBuilder.class.getMethod("buildConnectionProvider", String.class);
         return (ConnectionProvider) method.invoke(null, dbName);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
         throw new RuntimeException(e);
      }
   }
}
