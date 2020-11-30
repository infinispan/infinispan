package org.infinispan.cache.impl;

import java.util.concurrent.CompletableFuture;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * It invokes the {@link VisitableCommand} through this cache {@link AsyncInterceptorChain}.
 * <p>
 * It creates injected transactions and auto commits them, if the cache is transactional.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class InvocationHelper {

   private static final Log log = LogFactory.getLog(InvocationHelper.class);

   @Inject protected AsyncInterceptorChain invoker;
   @Inject protected InvocationContextFactory invocationContextFactory;
   @Inject protected TransactionManager transactionManager;
   @Inject protected Configuration config;
   @Inject protected BatchContainer batchContainer;
   @Inject protected BlockingManager blockingManager;
   private final ContextBuilder defaultBuilder = i -> createInvocationContextWithImplicitTransaction(i, false);

   private static void checkLockOwner(InvocationContext context, VisitableCommand command) {
      if (context.getLockOwner() == null && command instanceof RemoteLockCommand) {
         context.setLockOwner(((RemoteLockCommand) command).getKeyLockOwner());
      }
   }

   private static boolean isTxInjected(InvocationContext ctx) {
      return ctx.isInTxScope() && ((TxInvocationContext<?>) ctx).isImplicitTransaction();
   }

   /**
    * Same as {@link #invoke(ContextBuilder, VisitableCommand, int)} but using the default {@link ContextBuilder}.
    *
    * @param command  The {@link VisitableCommand} to invoke.
    * @param keyCount The number of keys affected by the {@code command}.
    * @param <T>      The return type.
    * @return The invocation result.
    */
   public <T> T invoke(VisitableCommand command, int keyCount) {
      InvocationContext ctx = createInvocationContextWithImplicitTransaction(keyCount, false);
      return invoke(ctx, command);
   }

   /**
    * Same as {@link #invoke(InvocationContext, VisitableCommand)} but using {@code builder} to build the {@link
    * InvocationContext} to use.
    *
    * @param builder  The {@link ContextBuilder} to create the {@link InvocationContext} to use.
    * @param command  The {@link VisitableCommand} to invoke.
    * @param keyCount The number of keys affected by the {@code command}.
    * @param <T>      The return type.
    * @return The invocation result.
    */
   public <T> T invoke(ContextBuilder builder, VisitableCommand command, int keyCount) {
      InvocationContext ctx = builder.create(keyCount);
      return invoke(ctx, command);
   }

   /**
    * Invokes the {@code command} using {@code context}.
    * <p>
    * This method blocks until the {@code command} finishes. Use {@link #invokeAsync(InvocationContext,
    * VisitableCommand)} for non-blocking.
    *
    * @param context The {@link InvocationContext} to use.
    * @param command The {@link VisitableCommand} to invoke.
    * @param <T>     The return type.
    * @return The invocation result.
    */
   public <T> T invoke(InvocationContext context, VisitableCommand command) {
      checkLockOwner(context, command);
      return isTxInjected(context) ?
             executeCommandWithInjectedTx(context, command) :
             doInvoke(context, command);
   }

   /**
    * Same as {@link #invoke(ContextBuilder, VisitableCommand, int)} but using the default {@link ContextBuilder}.
    *
    * @param command  The {@link VisitableCommand} to invoke.
    * @param keyCount The number of keys affected by the {@code command}.
    * @param <T>      The return type.
    * @return A {@link CompletableFuture} with the result.
    */
   public <T> CompletableFuture<T> invokeAsync(VisitableCommand command, int keyCount) {
      InvocationContext ctx = createInvocationContextWithImplicitTransaction(keyCount, false);
      return invokeAsync(ctx, command);
   }

   /**
    * Same as {@link #invoke(InvocationContext, VisitableCommand)} but using the {@link InvocationContext} created by
    * {@code builder}.
    *
    * @param builder  The {@link ContextBuilder} to create the {@link InvocationContext} to use.
    * @param command  The {@link VisitableCommand} to invoke.
    * @param keyCount The number of keys affected by the {@code command}.
    * @param <T>      The return type.
    * @return A {@link CompletableFuture} with the result.
    */
   public <T> CompletableFuture<T> invokeAsync(ContextBuilder builder, VisitableCommand command, int keyCount) {
      InvocationContext ctx = builder.create(keyCount);
      return invokeAsync(ctx, command);
   }

   /**
    * Invokes the {@code command} using {@code context} and returns a {@link CompletableFuture}.
    * <p>
    * The {@link CompletableFuture} is completed with the return value of the invocation.
    *
    * @param context The {@link InvocationContext} to use.
    * @param command The {@link VisitableCommand} to invoke.
    * @param <T>     The return type.
    * @return A {@link CompletableFuture} with the result.
    */
   public <T> CompletableFuture<T> invokeAsync(InvocationContext context, VisitableCommand command) {
      checkLockOwner(context, command);
      return isTxInjected(context) ?
             executeCommandAsyncWithInjectedTx(context, command) :
             doInvokeAsync(context, command);
   }

   /**
    * @return The default {@link ContextBuilder} implementation for write operations.
    */
   public ContextBuilder defaultContextBuilderForWrite() {
      return defaultBuilder;
   }

   /**
    * Creates an invocation context with an implicit transaction if it is required. An implicit transaction is created
    * if there is no current transaction and autoCommit is enabled.
    *
    * @param keyCount how many keys are expected to be changed
    * @return the invocation context
    */
   public InvocationContext createInvocationContextWithImplicitTransaction(int keyCount,
         boolean forceCreateTransaction) {
      boolean txInjected = false;
      TransactionConfiguration txConfig = config.transaction();
      if (txConfig.transactionMode().isTransactional()) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null && (forceCreateTransaction || txConfig.autoCommit())) {
            transaction = tryBegin();
            txInjected = true;
         }
         return invocationContextFactory.createInvocationContext(transaction, txInjected);
      } else {
         return invocationContextFactory.createInvocationContext(true, keyCount);
      }
   }

   @Override
   public String toString() {
      return "InvocationHelper{}";
   }

   private Transaction getOngoingTransaction() {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (transaction == null && config.invocationBatching().enabled()) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   private <T> T executeCommandWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      final T result;
      try {
         result = doInvoke(ctx, command);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      tryCommit();
      return result;
   }

   private <T> CompletableFuture<T> executeCommandAsyncWithInjectedTx(InvocationContext ctx, VisitableCommand command) {
      CompletableFuture<T> cf;
      final Transaction implicitTransaction;
      try {
         // interceptors must not access thread-local transaction anyway
         implicitTransaction = transactionManager.suspend();
         assert implicitTransaction != null;
         cf = doInvokeAsync(ctx, command);
      } catch (SystemException e) {
         throw new CacheException("Cannot suspend implicit transaction", e);
      } catch (Throwable e) {
         tryRollback();
         throw e;
      }
      return blockingManager.handleBlocking(cf, (result, throwable) -> {
         if (throwable != null) {
            try {
               implicitTransaction.rollback();
            } catch (SystemException e) {
               log.trace("Could not rollback", e);
               throwable.addSuppressed(e);
            }
            throw CompletableFutures.asCompletionException(throwable);
         }
         try {
            implicitTransaction.commit();
         } catch (Exception e) {
            log.couldNotCompleteInjectedTransaction(e);
            throw CompletableFutures.asCompletionException(e);
         }
         return result;
      }, ctx.getLockOwner()).toCompletableFuture();
   }

   private Transaction tryBegin() {
      if (transactionManager == null) {
         return null;
      }
      try {
         transactionManager.begin();
         final Transaction transaction = getOngoingTransaction();
         if (log.isTraceEnabled()) {
            log.tracef("Implicit transaction started! Transaction: %s", transaction);
         }
         return transaction;
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException("Unable to begin implicit transaction.", e);
      }
   }

   private void tryRollback() {
      try {
         if (transactionManager != null) {
            transactionManager.rollback();
         }
      } catch (Throwable t) {
         if (log.isTraceEnabled()) {
            log.trace("Could not rollback", t);//best effort
         }
      }
   }

   private void tryCommit() {
      if (transactionManager == null) {
         return;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Committing transaction as it was implicit: %s", getOngoingTransaction());
      }
      try {
         transactionManager.commit();
      } catch (Throwable e) {
         log.couldNotCompleteInjectedTransaction(e);
         throw new CacheException("Could not commit implicit transaction", e);
      }
   }

   private <T> CompletableFuture<T> doInvokeAsync(InvocationContext ctx, VisitableCommand command) {
      //noinspection unchecked
      return (CompletableFuture<T>) invoker.invokeAsync(ctx, command);
   }

   private <T> T doInvoke(InvocationContext ctx, VisitableCommand command) {
      //noinspection unchecked
      return (T) invoker.invoke(ctx, command);
   }
}
