package org.infinispan.client.hotrod.impl.transaction;

import static org.infinispan.commons.tx.Util.transactionStatusToString;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.operations.PrepareTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.entry.Modification;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;
import org.infinispan.commons.CacheException;

/**
 * A {@link TransactionTable} that registers the {@link RemoteCache} as a {@link Synchronization} in the transaction.
 * <p>
 * Only a single {@link Synchronization} is registered even if multiple {@link RemoteCache}s interact with the same
 * transaction.
 * <p>
 * When more than one {@link RemoteCache} is involved in the {@link Transaction}, the prepare, commit and rollback
 * requests are sent sequential and they are ordered by the {@link RemoteCache}'s name.
 * <p>
 * If a {@link RemoteCache} is read-only, the commit/rollback isn't invoked.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class SyncModeTransactionTable implements TransactionTable {

   private static final Log log = LogFactory.getLog(SyncModeTransactionTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<Transaction, SynchronizationAdapter> registeredTransactions = new ConcurrentHashMap<>();
   private final UUID uuid = UUID.randomUUID();
   private final Function<Transaction, SynchronizationAdapter> constructor = this::createSynchronizationAdapter;
   private final Consumer<Transaction> cleanup = registeredTransactions::remove;

   @Override
   public <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCacheImpl<K, V> txRemoteCache, Transaction tx) {
      //registers a synchronization if it isn't done yet.
      SynchronizationAdapter adapter = registeredTransactions.computeIfAbsent(tx, constructor);
      //registers the cache.
      TransactionContext<K, V> context = adapter.registerCache(txRemoteCache);
      if (trace) {
         log.tracef("Xid=%s retrieving context: %s", adapter.xid, context);
      }
      return context;
   }

   /**
    * Creates and registers the {@link SynchronizationAdapter} in the {@link Transaction}.
    */
   private SynchronizationAdapter createSynchronizationAdapter(Transaction transaction) {
      SynchronizationAdapter adapter = new SynchronizationAdapter(transaction, cleanup, RemoteXid.create(uuid));
      try {
         transaction.registerSynchronization(adapter);
      } catch (RollbackException | SystemException e) {
         throw new CacheException(e);
      }
      if (trace) {
         log.tracef("Registered synchronization for transaction %s. Sync=%s", transaction, adapter);
      }
      return adapter;
   }

   private static class SynchronizationAdapter implements Synchronization {

      private final Map<String, TransactionContext<?, ?>> registeredCaches = new ConcurrentSkipListMap<>();
      private final Collection<TransactionContext<?, ?>> preparedCaches = new LinkedList<>();
      private final Transaction transaction;
      private final Consumer<Transaction> cleanupTask;
      private final RemoteXid xid;

      private SynchronizationAdapter(Transaction transaction, Consumer<Transaction> cleanupTask, RemoteXid xid) {
         this.transaction = transaction;
         this.cleanupTask = cleanupTask;
         this.xid = xid;
      }

      @Override
      public String toString() {
         return "SynchronizationAdapter{" +
               "registeredCaches=" + registeredCaches.keySet() +
               ", transaction=" + transaction +
               ", xid=" + xid +
               '}';
      }

      @Override
      public void beforeCompletion() {
         if (trace) {
            log.tracef("BeforeCompletion(xid=%s, remote-caches=%s)", xid, registeredCaches.keySet());
         }
         if (isMarkedRollback()) {
            return;
         }
         for (TransactionContext<?, ?> txContext : registeredCaches.values()) {
            switch (prepareContext(txContext)) {
               case XAResource.XA_OK:
                  preparedCaches.add(txContext);
                  break;
               case XAResource.XA_RDONLY:
                  break; //read only tx.
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  markAsRollback();
                  return;
               default:
                  //any other code we need to rollback
                  //we may need to send the rollback later
                  preparedCaches.add(txContext);
                  markAsRollback();
                  return;
            }
         }
      }

      @Override
      public void afterCompletion(int status) {
         if (trace) {
            log.tracef("AfterCompletion(xid=%s, status=%s, remote-caches=%s)", xid, transactionStatusToString(status),
                  preparedCaches);
         }
         //we only use the preparedCaches. the remaining caches are read-only or they didn't contact the server.
         try {
            if (status == Status.STATUS_COMMITTED) {
               for (TransactionContext<?,?> ctx : preparedCaches) {
                  commitContext(ctx);
               }
            } else {
               for (TransactionContext<?,?> ctx : preparedCaches) {
                  rollbackContext(ctx);
               }
            }
         } finally {
            cleanupTask.accept(transaction);
         }
      }

      private void rollbackContext(TransactionContext<?, ?> context) {
         try {
            if (trace) {
               log.tracef("Rolling-back transaction xid=%s, remote-cache=%s", xid, context.getCacheName());
            }
            CompleteTransactionOperation operation = context.getOperationsFactory()
                  .newCompleteTransactionOperation(xid, false);
            operation.execute().get();
         } catch (Exception e) {
            log.debug("Exception while rollback.", e);
         }
      }

      private void commitContext(TransactionContext<?, ?> context) {
         try {
            if (trace) {
               log.tracef("Committing transaction xid=%s, remote-cache=%s", xid, context.getCacheName());
            }
            CompleteTransactionOperation operation = context.getOperationsFactory()
                  .newCompleteTransactionOperation(xid, true);
            operation.execute().get();
         } catch (Exception e) {
            log.debug("Exception while commit.", e);
         }
      }

      private void markAsRollback() {
         try {
            transaction.setRollbackOnly();
         } catch (SystemException e) {
            log.debug("Exception in markAsRollback", e);
         }
      }

      /**
       * Prepares the {@link Transaction} in the server and returns the {@link XAResource} code.
       * <p>
       * A special value {@link Integer#MIN_VALUE} is used to signal an error before contacting the server (for example,
       * it wasn't able to marshall the key/value)
       */
      private int prepareContext(TransactionContext<?, ?> context) {
         PrepareTransactionOperation operation;
         Collection<Modification> modifications;
         try {
            modifications = context.toModification();
            if (trace) {
               log.tracef("Preparing transaction xid=%s, remote-cache=%s, modification-size=%d", xid,
                     context.getCacheName(), modifications.size());
            }
            if (modifications.isEmpty()) {
               return XAResource.XA_RDONLY;
            }
         } catch (Exception e) {
            return Integer.MIN_VALUE;
         }
         try {
            int xaReturnCode;
            do {
               operation = context.getOperationsFactory().newPrepareTransactionOperation(xid, false, modifications);
               xaReturnCode = operation.execute().get();
            } while (operation.shouldRetry());
            return xaReturnCode;
         } catch (Exception e) {
            return XAException.XA_RBROLLBACK;
         }
      }

      private boolean isMarkedRollback() {
         try {
            return transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK;
         } catch (SystemException e) {
            log.debug("Exception in isMarkedRollback", e);
            //lets assume not.
            return false;
         }
      }

      private <K, V> TransactionContext<K, V> registerCache(TransactionalRemoteCacheImpl<K, V> txRemoteCache) {
         //noinspection unchecked
         return (TransactionContext<K, V>) registeredCaches
               .computeIfAbsent(txRemoteCache.getName(), s -> createTxContext(txRemoteCache));
      }

      private <K, V> TransactionContext<K, V> createTxContext(TransactionalRemoteCacheImpl<K, V> remoteCache) {
         if (trace) {
            log.tracef("Registering remote cache '%s' for transaction xid=%s", remoteCache.getName(), xid);
         }
         return new TransactionContext<>(remoteCache.keyMarshaller(), remoteCache.valueMarshaller(),
               remoteCache.getOperationsFactory(), remoteCache.getName());
      }
   }
}
