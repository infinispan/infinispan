package org.infinispan.functional.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.encoding.DataConversion;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Abstract functional map, providing implementations for some of the shared methods.
 *
 * @since 8.0
 */
abstract class AbstractFunctionalMap<K, V> implements FunctionalMap<K, V> {
   private static final Log log = LogFactory.getLog(FunctionalMap.class);

   protected final FunctionalMapImpl<K, V> fmap;
   protected final Params params;

   private final boolean transactional;
   private final boolean autoCommit;
   private final BatchContainer batchContainer;
   private final TransactionManager transactionManager;

   protected final DataConversion keyDataConversion;
   protected final DataConversion valueDataConversion;

   protected AbstractFunctionalMap(Params params, FunctionalMapImpl<K, V> fmap) {
      this.fmap = fmap;
      Configuration config = fmap.cache.getCacheConfiguration();
      transactional = config.transaction().transactionMode().isTransactional();
      autoCommit = config.transaction().autoCommit();
      transactionManager = transactional ? fmap.cache.getTransactionManager() : null;
      batchContainer = transactional && config.invocationBatching().enabled() ? fmap.cache.getBatchContainer() : null;
      this.params = params;
      this.keyDataConversion = fmap.cache.getKeyDataConversion();
      this.valueDataConversion = fmap.cache.getValueDataConversion();
   }

   @Override
   public String getName() {
      return "";
   }

   @Override
   public ComponentStatus getStatus() {
      return fmap.getStatus();
   }

   @Override
   public void close() throws Exception {
      fmap.close();
   }

   protected InvocationContext getInvocationContext(boolean isWrite, int keyCount) {
      InvocationContext invocationContext;
      boolean txInjected = false;
      if (transactional) {
         Transaction transaction = getOngoingTransaction();
         if (transaction == null && autoCommit && transactionManager != null) {
            try {
               transactionManager.begin();
            } catch (RuntimeException e) {
               throw e;
            } catch (Exception e) {
               throw new CacheException("Unable to begin implicit transaction.", e);
            }
            transaction = getOngoingTransaction();
            txInjected = true;
         }
         invocationContext = fmap.invCtxFactory.createInvocationContext(transaction, txInjected);
      } else {
         invocationContext = fmap.invCtxFactory.createInvocationContext(isWrite, keyCount);
      }
      // Functional map has no way to lock key so we only have to add lock owner for writes
      if (isWrite && fmap.lockOwner != null) {
         invocationContext.setLockOwner(fmap.lockOwner);
      }
      return invocationContext;
   }

   private Transaction getOngoingTransaction() {
      try {
         Transaction transaction = null;
         if (transactionManager != null) {
            transaction = transactionManager.getTransaction();
            if (transaction == null && batchContainer != null) {
               transaction = batchContainer.getBatchTransaction();
            }
         }
         return transaction;
      } catch (SystemException e) {
         throw new CacheException("Unable to get transaction", e);
      }
   }

   protected <T> CompletableFuture<T> invokeAsync(InvocationContext ctx, VisitableCommand cmd) {
      CompletableFuture<T> cf;
      boolean isImplicitTx = ctx.isInTxScope() && ((TxInvocationContext) ctx).isImplicitTransaction();
      final Transaction implicitTransaction;
      try {
         // interceptors must not access thread-local transaction anyway
         if (isImplicitTx) {
            implicitTransaction = transactionManager.suspend();
            assert implicitTransaction != null;
         } else {
            implicitTransaction = null;
         }
         cf = (CompletableFuture<T>) fmap.chain.invokeAsync(ctx, cmd);
      } catch (SystemException e) {
         throw new CacheException("Cannot suspend implicit transaction", e);
      } catch (Throwable t) {
         if (isImplicitTx) {
            try {
               if (transactionManager != null) transactionManager.rollback();
            } catch (Throwable t2) {
               log.trace("Could not rollback", t2);//best effort
               t.addSuppressed(t2);
            }
         }
         throw t;
      }
      if (isImplicitTx) {
         return cf.handle((result, throwable) -> {
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
         });
      } else {
         return cf;
      }
   }

   protected Set<?> encodeKeys(Set<? extends K> keys) {
      return keys.stream().map(k -> keyDataConversion.toStorage(k)).collect(Collectors.toSet());
   }

   protected Map<?, ?> encodeEntries(Map<? extends K, ? extends V> entries) {
      Map encodedEntries = new HashMap<>();
      entries.entrySet().forEach(e -> {
         Object keyEncoded = keyDataConversion.toStorage(e.getKey());
         Object valueEncoded = valueDataConversion.toStorage(e.getValue());
         encodedEntries.put(keyEncoded, valueEncoded);
      });
      return encodedEntries;
   }
}
