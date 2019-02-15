package org.infinispan.client.hotrod.impl.transaction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.transaction.recovery.RecoveryIterator;
import org.infinispan.client.hotrod.impl.transaction.recovery.RecoveryManager;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheException;

/**
 * A {@link TransactionTable} that registers the {@link RemoteCache} as a {@link XAResource} in the transaction.
 * <p>
 * Only a single {@link XAResource} is registered even if multiple {@link RemoteCache}s interact with the same
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
public class XaModeTransactionTable extends AbstractTransactionTable {

   private static final Log log = LogFactory.getLog(XaModeTransactionTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Map<Transaction, XaAdapter> registeredTransactions = new ConcurrentHashMap<>();
   private final RecoveryManager recoveryManager = new RecoveryManager();
   private final Function<Transaction, XaAdapter> constructor = this::createTransactionData;

   public XaModeTransactionTable(long timeout) {
      super(timeout);
   }

   @Override
   public <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCacheImpl<K, V> txRemoteCache, Transaction tx) {
      XaAdapter xaAdapter = registeredTransactions.computeIfAbsent(tx, constructor);
      return xaAdapter.registerCache(txRemoteCache);
   }

   public XAResource getXaResource() {
      return new XaAdapter(null, getTimeout());
   }

   @Override
   Log getLog() {
      return log;
   }

   @Override
   boolean isTraceLogEnabled() {
      return trace;
   }

   private XaAdapter createTransactionData(Transaction transaction) {
      XaAdapter xaAdapter = new XaAdapter(transaction, getTimeout());
      try {
         transaction.enlistResource(xaAdapter);
      } catch (RollbackException | SystemException e) {
         throw new CacheException(e);
      }
      return xaAdapter;
   }

   private class XaAdapter implements XAResource {
      private final Transaction transaction;
      private final Map<String, TransactionContext<?, ?>> registeredCaches;
      private volatile Xid currentXid;
      private volatile RecoveryIterator iterator;
      private long timeoutMs;
      private boolean needsRecovery = false;

      private XaAdapter(Transaction transaction, long timeout) {
         this.transaction = transaction;
         this.timeoutMs = timeout;
         this.registeredCaches = transaction == null ?
                                 Collections.emptyMap() :
                                 new ConcurrentSkipListMap<>();
      }

      @Override
      public String toString() {
         return "XaResource{" +
                "transaction=" + transaction +
                ", caches=" + registeredCaches.keySet() +
                '}';
      }

      @Override
      public void start(Xid xid, int flags) throws XAException {
         if (trace) {
            log.tracef("XaResource.start(%s, %s)", xid, flags);
         }
         switch (flags) {
            case TMJOIN:
            case TMRESUME: // means joining a previously seen transaction. it should exist otherwise throw XAER_NOTA
               if (currentXid != null && !currentXid.equals(xid)) {
                  //we have a running tx but it isn't the same tx
                  throw new XAException(XAException.XAER_OUTSIDE);
               }
               assertStartInvoked();
               break;
            case TMNOFLAGS: //new transaction.
               if (currentXid != null) {
                  //we have a running transaction already!
                  throw new XAException(XAException.XAER_RMERR);
               }
               currentXid = xid;
               break;
            default: //no other flag should be used.
               throw new XAException(XAException.XAER_RMERR);
         }
      }

      @Override
      public void end(Xid xid, int flags) throws XAException {
         if (trace) {
            log.tracef("XaResource.end(%s, %s)", xid, flags);
         }
         assertStartInvoked();
         assertSameXid(xid, XAException.XAER_OUTSIDE);
      }

      @Override
      public int prepare(Xid xid) throws XAException {
         if (trace) {
            log.tracef("XaResource.prepare(%s)", xid);
         }
         assertStartInvoked();
         assertSameXid(xid, XAException.XAER_INVAL);
         return internalPrepare();
      }

      @Override
      public void commit(Xid xid, boolean onePhaseCommit) throws XAException {
         if (trace) {
            log.tracef("XaResource.commit(%s, %s)", xid, onePhaseCommit);
         }
         if (currentXid == null) {
            //no transaction running. we are doing some recovery work
            currentXid = xid;
         } else {
            assertSameXid(xid, XAException.XAER_INVAL);
      }

         try {
            if (onePhaseCommit) {
               onePhaseCommit();
            } else {
               internalCommit();
      }
         } finally {
            cleanup();
         }
      }

      @Override
      public void rollback(Xid xid) throws XAException {
         if (trace) {
            log.tracef("XaResource.rollback(%s)", xid);
         }
         boolean ignoreNoTx = true;
         if (currentXid == null) {
            //no transaction running. we are doing some recovery work
            currentXid = xid;
            ignoreNoTx = false;
         } else {
            assertSameXid(xid, XAException.XAER_INVAL);
         }
         try {
            internalRollback(ignoreNoTx);
         } finally {
            cleanup();
         }
      }

      @Override
      public boolean isSameRM(XAResource xaResource) {
         if (trace) {
            log.tracef("XaResource.isSameRM(%s)", xaResource);
         }
         return xaResource instanceof XaAdapter && Objects.equals(transaction, ((XaAdapter) xaResource).transaction);
      }

      @Override
      public void forget(Xid xid) {
         if (trace) {
            log.tracef("XaResource.forget(%s)", xid);
         }
         recoveryManager.forgetTransaction(xid);
         forgetTransaction(xid);
      }

      @Override
      public Xid[] recover(int flags) throws XAException {
         if (trace) {
            log.tracef("XaResource.recover(%s)", flags);
         }
         RecoveryIterator it = this.iterator;
         if ((flags & XAResource.TMSTARTRSCAN) != 0) {
            if (it == null) {
               it = recoveryManager.startScan(fetchPreparedTransactions());
               iterator = it;
            } else {
               //we have an iteration in progress.
               throw new XAException(XAException.XAER_INVAL);
            }
         }
         if ((flags & XAResource.TMENDRSCAN) != 0) {
            if (it == null) {
               //we don't have an iteration in progress
               throw new XAException(XAException.XAER_INVAL);
            } else {
               iterator.finish(timeoutMs);
               iterator = null;
            }
         }
         if (it == null) {
            //we don't have an iteration in progress
            throw new XAException(XAException.XAER_INVAL);
         }
         return it.next();
      }

      @Override
      public boolean setTransactionTimeout(int timeoutSeconds) {
         this.timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);
         return true;
      }

      @Override
      public int getTransactionTimeout() {
         return (int) TimeUnit.MILLISECONDS.toSeconds(timeoutMs);
      }

      private void assertStartInvoked() throws XAException {
         if (currentXid == null) {
            //we don't have a transaction
            throw new XAException(XAException.XAER_NOTA);
         }
      }

      private void assertSameXid(Xid otherXid, int xaErrorCode) throws XAException {
         if (!currentXid.equals(otherXid)) {
            //we have another tx running
            throw new XAException(xaErrorCode);
         }
      }

      private void internalRollback(boolean ignoreNoTx) throws XAException {
         int xa_code = completeTransaction(currentXid, false);
         switch (xa_code) {
            case XAResource.XA_OK:       //no issues
            case XAResource.XA_RDONLY:   //no issues
            case XAException.XA_HEURRB:  //heuristically rolled back
               break;
            case XAException.XAER_NOTA:  //no transaction in server. already rolled-back or never reached the server
               if (ignoreNoTx) {
                  break;
               }
            default:
               throw new XAException(xa_code);
         }
      }

      private void internalCommit() throws XAException {
         int xa_code = completeTransaction(currentXid, true);
         switch (xa_code) {
            case XAResource.XA_OK:       //no issues
            case XAResource.XA_RDONLY:   //no issues
            case XAException.XA_HEURCOM:  //heuristically committed
               break;
            default:
               throw new XAException(xa_code);
         }
      }

      private int internalPrepare() throws XAException {
         boolean readOnly = true;
         for (TransactionContext<?, ?> ctx : registeredCaches.values()) {
            switch (ctx.prepareContext(currentXid, false, timeoutMs)) {
               case XAResource.XA_OK:
                  readOnly = false;
                  break;
               case XAResource.XA_RDONLY:
                  break; //read only tx.
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  throw new XAException(XAException.XA_RBROLLBACK);
               default:
                  //any other code we need to rollback
                  //we may need to send the rollback later
                  throw new XAException(XAException.XA_RBROLLBACK);
            }
         }
         if (needsRecovery) {
            recoveryManager.addTransaction(currentXid);
         }
         return readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK;
      }

      private void onePhaseCommit() throws XAException {
         //check only the write transaction to know who is the last cache to commit
         List<TransactionContext<?, ?>> txCaches = registeredCaches.values().stream()
               .filter(TransactionContext::isReadWrite)
               .collect(Collectors.toList());
         int size = txCaches.size();
         if (size == 0) {
            return;
         }
         boolean commit = true;

         outer:
         for (int i = 0; i < size - 1; ++i) {
            TransactionContext<?, ?> ctx = txCaches.get(i);
            switch (ctx.prepareContext(currentXid, false, timeoutMs)) {
               case XAResource.XA_OK:
                  break;
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  commit = false;
                  break outer;
               default:
                  //any other code we need to rollback
                  //we may need to send the rollback later
                  commit = false;
                  break outer;
            }
         }

         //last resource one phase commit!
         if (commit && txCaches.get(size - 1).prepareContext(currentXid, true, timeoutMs) == XAResource.XA_OK) {
            internalCommit(); //commit other caches
         } else {
            internalRollback(true);
            throw new XAException(XAException.XA_RBROLLBACK); //tell TM to rollback
         }
      }

      private <K, V> TransactionContext<K, V> registerCache(TransactionalRemoteCacheImpl<K, V> txRemoteCache) {
         if (currentXid == null) {
            throw new CacheException("XaResource wasn't invoked!");
         }
         needsRecovery |= txRemoteCache.isRecoveryEnabled();
         //noinspection unchecked
         return (TransactionContext<K, V>) registeredCaches
               .computeIfAbsent(txRemoteCache.getName(), s -> createTxContext(txRemoteCache));
      }

      private <K, V> TransactionContext<K, V> createTxContext(TransactionalRemoteCacheImpl<K, V> remoteCache) {
         if (trace) {
            log.tracef("Registering remote cache '%s' for transaction xid=%s", remoteCache.getName(), currentXid);
         }
         return new TransactionContext<>(remoteCache.keyMarshaller(), remoteCache.valueMarshaller(),
               remoteCache.getOperationsFactory(), remoteCache.getName(), remoteCache.isRecoveryEnabled());
      }

      private void cleanup() {
         //if null, it was created by RemoteCacheManager.getXaResource()
         if (transaction != null) {
            //enlisted with a cache
            registeredTransactions.remove(transaction);
            //this instance can be used for recovery. we need at least one cache registered in order to access
            // the operation factory
            registeredCaches.values().forEach(TransactionContext::cleanupEntries);
         }
         recoveryManager.forgetTransaction(currentXid); //transaction completed, we can remove it from recovery
         currentXid = null;
      }
   }
}
