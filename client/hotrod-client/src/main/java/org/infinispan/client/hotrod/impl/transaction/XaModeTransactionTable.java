package org.infinispan.client.hotrod.impl.transaction;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.RemoteCache;
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
public class XaModeTransactionTable implements TransactionTable {

   private static final Log log = LogFactory.getLog(XaModeTransactionTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Xid[] NO_XIDS = new Xid[0];

   private final Map<Transaction, XaAdapter> registeredTransactions = new ConcurrentHashMap<>();
   private final Function<Transaction, XaAdapter> constructor = this::createTransactionData;

   @Override
   public <K, V> TransactionContext<K, V> enlist(TransactionalRemoteCacheImpl<K, V> txRemoteCache, Transaction tx) {
      XaAdapter xaAdapter = registeredTransactions.computeIfAbsent(tx, constructor);
      return xaAdapter.registerCache(txRemoteCache);
   }

   private XaAdapter createTransactionData(Transaction transaction) {
      XaAdapter xaAdapter = new XaAdapter(transaction);
      try {
         transaction.enlistResource(xaAdapter);
      } catch (RollbackException | SystemException e) {
         throw new CacheException(e);
      }
      return xaAdapter;
   }

   private class XaAdapter implements XAResource {
      private final Transaction transaction;
      private final Map<String, TransactionContext<?, ?>> registeredCaches = new ConcurrentSkipListMap<>();
      private final List<TransactionContext<?, ?>> preparedCaches = new LinkedList<>();
      private volatile Xid currentXid;

      private XaAdapter(Transaction transaction) {
         this.transaction = transaction;
      }

      @Override
      public String toString() {
         return "TransactionData{" +
               "xid=" + currentXid +
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
         if (flags == XAResource.TMFAIL) {
            //tx will rollback. we can cleanup immediately
            registeredCaches.clear();
         }
         //no other work to be done.
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
         assertStartInvoked();
         assertSameXid(xid, XAException.XAER_INVAL);
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
         assertStartInvoked();
         assertSameXid(xid, XAException.XAER_INVAL);
         try {
            internalRollback();
         } finally {
            cleanup();
         }
      }

      @Override
      public boolean isSameRM(XAResource xaResource) {
         if (trace) {
            log.tracef("XaResource.isSameRM(%s)", xaResource);
         }
         return xaResource == this;
      }

      @Override
      public void forget(Xid xid) {
         if (trace) {
            log.tracef("XaResource.forget(%s)", xid);
         }
         //no-op
      }

      @Override
      public Xid[] recover(int i) {
         //no recovery yet
         return NO_XIDS;
      }

      @Override
      public boolean setTransactionTimeout(int i) {
         //not supported yet
         return false;
      }

      @Override
      public int getTransactionTimeout() {
         //not supported yet
         return 0;
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

      private void internalRollback() throws XAException {
         boolean hasCommit = false;
         boolean hasRollback = false;
         boolean alreadyForgot = false;
         boolean unknown = false;
         int unknownErrorCode = 0;

         for (TransactionContext<?, ?> ctx : preparedCaches) {
            int xa_code = ctx.complete(currentXid, false);
            switch (xa_code) {
               case XAResource.XA_OK:       //no issues
               case XAResource.XA_RDONLY:   //no issues
               case XAException.XA_HEURRB:  //heuristically rolled back
                  hasRollback = true;
                  break;
               case XAException.XAER_NOTA:
                  //tx already forgotten by the server.
                  alreadyForgot = true;
                  break;
               case XAException.XA_HEURMIX: //completed but not sure how
               case XAException.XA_HEURHAZ: //heuristically committed and rolled back
                  hasCommit = true;
                  hasRollback = true;
                  break;
               case XAException.XA_HEURCOM: //heuristically committed
                  hasCommit = true;
                  break;
               default:
                  //other error codes
                  unknown = true;
                  unknownErrorCode = xa_code;
                  break;
            }
         }

         if (!hasCommit && !hasRollback) {
            //nothing committed neither rolled back
            if (alreadyForgot) {
               //no tx
               throw new XAException(XAException.XAER_NOTA);
            } else if (unknown) {
               //unknown. we throw what we received
               throw new XAException(unknownErrorCode);
            }
         } else if (hasCommit && hasRollback) {
            //we have something committed and something rolled back
            throw new XAException(XAException.XA_HEURMIX);
         } else if (hasCommit) {
            //we only got commits
            throw new XAException(XAException.XA_HEURCOM);
         }
         //we only got rollbacks
      }

      private void internalCommit() throws XAException {
         boolean hasCommit = false;
         boolean hasRollback = false;
         boolean alreadyForgot = false;
         boolean unknown = false;
         int unknownErrorCode = 0;

         for (TransactionContext<?, ?> ctx : preparedCaches) {
            int xa_code = ctx.complete(currentXid, true);
            switch (xa_code) {
               case XAResource.XA_OK:       //no issues
               case XAResource.XA_RDONLY:   //no issues
               case XAException.XA_HEURCOM: //heuristically committed
                  hasCommit = true;
                  break;
               case XAException.XAER_NOTA:
                  //tx already forgotten by the server.
                  alreadyForgot = true;
                  break;
               case XAException.XA_HEURMIX: //completed. not sure how
               case XAException.XA_HEURHAZ: //heuristically committed and rolled back
                  hasCommit = true;
                  hasRollback = true;
                  break;
               case XAException.XA_HEURRB: //heuristically rolled back
                  hasRollback = true;
                  break;
               default:
                  //other error codes
                  unknown = true;
                  unknownErrorCode = xa_code;
                  break;
            }
         }

         if (!hasCommit && !hasRollback) {
            //nothing committed neither rolled back
            if (alreadyForgot) {
               //no tx
               throw new XAException(XAException.XAER_NOTA);
            } else if (unknown) {
               //unknown. we throw what we received
               throw new XAException(unknownErrorCode);
            }
         } else if (hasCommit && hasRollback) {
            //we have something committed and something rolled back
            throw new XAException(XAException.XA_HEURMIX);
         } else if (hasRollback) {
            //rollbacks only
            throw new XAException(XAException.XA_HEURRB);
         }
         // commits only.
      }

      private int internalPrepare() throws XAException {
         boolean readOnly = true;
         for (TransactionContext<?, ?> ctx : registeredCaches.values()) {
            switch (ctx.prepareContext(currentXid, false)) {
               case XAResource.XA_OK:
                  readOnly = false;
                  preparedCaches.add(ctx);
                  break;
               case XAResource.XA_RDONLY:
                  break; //read only tx.
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  throw new XAException(XAException.XA_RBROLLBACK);
               default:
                  //any other code we need to rollback
                  //we may need to send the rollback later
                  preparedCaches.add(ctx);
                  throw new XAException(XAException.XA_RBROLLBACK);
            }
         }
         return readOnly ? XAResource.XA_RDONLY : XAResource.XA_OK;
      }

      private void onePhaseCommit() throws XAException {
         //check only the write transaction to know who is the last cache to commit
         List<TransactionContext<?,?>> txCaches = registeredCaches.values().stream()
               .filter(TransactionContext::isReadWrite)
               .collect(Collectors.toList());
         int size = txCaches.size();
         if (size == 0) {
            return;
         }
         boolean commit = true;

         outer: for (int i = 0; i < size - 1; ++i) {
            TransactionContext<?,?> ctx = txCaches.get(i);
            switch (ctx.prepareContext(currentXid, false)) {
               case XAResource.XA_OK:
                  preparedCaches.add(ctx);
                  break;
               case Integer.MIN_VALUE:
                  //signals a marshaller error of key or value. the server wasn't contacted
                  commit = false;
                  break outer;
               default:
                  //any other code we need to rollback
                  //we may need to send the rollback later
                  preparedCaches.add(ctx);
                  commit = false;
                  break outer;
            }
         }

         //last resource one phase commit!
         if (commit && txCaches.get(size - 1).prepareContext(currentXid, true) == XAResource.XA_OK) {
            internalCommit(); //commit other caches
         } else {
            internalRollback();
            throw new XAException(XAException.XA_RBROLLBACK); //tell TM to rollback
         }
      }

      private <K, V> TransactionContext<K, V> registerCache(TransactionalRemoteCacheImpl<K, V> txRemoteCache) {
         if (currentXid == null) {
            throw new CacheException("XaResource wasn't invoked!");
         }
         //noinspection unchecked
         return (TransactionContext<K, V>) registeredCaches
               .computeIfAbsent(txRemoteCache.getName(), s -> createTxContext(txRemoteCache));
      }

      private <K, V> TransactionContext<K, V> createTxContext(TransactionalRemoteCacheImpl<K, V> remoteCache) {
         if (trace) {
            log.tracef("Registering remote cache '%s' for transaction xid=%s", remoteCache.getName(), currentXid);
         }
         return new TransactionContext<>(remoteCache.keyMarshaller(), remoteCache.valueMarshaller(),
               remoteCache.getOperationsFactory(), remoteCache.getName());
      }

      private void cleanup() {
         //TODO use proper method!
         if (!preparedCaches.isEmpty()) {
            preparedCaches.get(0).forget(currentXid);
         }
         registeredTransactions.remove(transaction);
         registeredCaches.clear();
         preparedCaches.clear();
         currentXid = null;
      }
   }
}
