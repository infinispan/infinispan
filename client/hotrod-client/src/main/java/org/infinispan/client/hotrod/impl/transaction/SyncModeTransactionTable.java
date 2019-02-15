package org.infinispan.client.hotrod.impl.transaction;

import static org.infinispan.commons.tx.Util.transactionStatusToString;

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
import javax.transaction.xa.XAResource;

import org.infinispan.client.hotrod.RemoteCache;
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
public class SyncModeTransactionTable extends AbstractTransactionTable {

   private static final Log log = LogFactory.getLog(SyncModeTransactionTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<Transaction, SynchronizationAdapter> registeredTransactions = new ConcurrentHashMap<>();
   private final UUID uuid = UUID.randomUUID();
   private final Consumer<Transaction> cleanup = registeredTransactions::remove;
   private final Function<Transaction, SynchronizationAdapter> constructor = this::createSynchronizationAdapter;

   public SyncModeTransactionTable(long timeout) {
      super(timeout);
   }

   @Override
   Log getLog() {
      return log;
   }

   @Override
   boolean isTraceLogEnabled() {
      return trace;
   }

   @Override
   public <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCacheImpl<K, V> txRemoteCache, Transaction tx) {
      assertStartedAndReturnFactory();
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
      SynchronizationAdapter adapter = new SynchronizationAdapter(transaction, RemoteXid.create(uuid));
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

   private class SynchronizationAdapter implements Synchronization {

      private final Map<String, TransactionContext<?, ?>> registeredCaches = new ConcurrentSkipListMap<>();
      private final Transaction transaction;
      private final RemoteXid xid;

      private SynchronizationAdapter(Transaction transaction, RemoteXid xid) {
         this.transaction = transaction;
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
            switch (txContext.prepareContext(xid, false, getTimeout())) {
               case XAResource.XA_OK:
               case XAResource.XA_RDONLY:
                  break; //read only tx.
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  markAsRollback();
                  return;
               default:
                  markAsRollback();
                  return;
            }
         }
      }

      @Override
      public void afterCompletion(int status) {
         if (trace) {
            log.tracef("AfterCompletion(xid=%s, status=%s, remote-caches=%s)", xid, transactionStatusToString(status),
                  registeredCaches.keySet());
         }
         //the server commits everything when the first request arrives.
         try {
            boolean commit = status == Status.STATUS_COMMITTED;
            completeTransaction(xid, commit);
         } finally {
            forgetTransaction(xid);
            cleanup.accept(transaction);
         }
      }

      private void markAsRollback() {
         try {
            transaction.setRollbackOnly();
         } catch (SystemException e) {
            log.debug("Exception in markAsRollback", e);
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
               remoteCache.getOperationsFactory(), remoteCache.getName(), false);
      }
   }
}
