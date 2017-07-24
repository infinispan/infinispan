package org.infinispan.server.hotrod.tx;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ByteString;

/**
 * A Transaction Table for client transaction.
 * <p>
 * It stores the global state of a transaction and the map between the {@link Xid} and {@link Transaction}'s run
 * locally.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class ServerTransactionTable {

   private static final Log log = LogFactory.getLog(ServerTransactionTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<CacheXid, TxState> globalTxTable;
   private final Map<Xid, EmbeddedTransaction> localTxTable;
   private final ByteString cacheName;
   private final ClientAddress clientAddress;

   public ServerTransactionTable(Map<CacheXid, TxState> globalTxTable, ByteString cacheName,
         Address address) {
      this.cacheName = cacheName;
      this.localTxTable = new ConcurrentHashMap<>();
      this.globalTxTable = globalTxTable;
      clientAddress = new ClientAddress(address);
   }

   ClientAddress getClientAddress() {
      return clientAddress;
   }

   /**
    * Removes the {@code xid} from the global and local transaction table.
    */
   void removeGlobalStateAndLocalTx(XidImpl xid) {
      TxState oldState = globalTxTable.remove(new CacheXid(cacheName, xid));
      EmbeddedTransaction oldTx = localTxTable.remove(xid);
      if (trace) {
         log.tracef("[%s] Removed state=%s and tx=%s", xid, oldState, oldTx);
      }
   }

   /**
    * @return The local {@link EmbeddedTransaction} associated to the {@code xid}.
    */
   EmbeddedTransaction getLocalTx(Xid xid) {
      return localTxTable.get(xid);
   }

   /**
    * Removes the local {@link EmbeddedTransaction} associated to {@code xid}.
    */
   void removeLocalTx(Xid xid) {
      EmbeddedTransaction tx = localTxTable.remove(xid);
      if (trace) {
         log.tracef("[%s] Removed tx=%s", xid, tx);
      }
   }

   /**
    * Adds the {@link TxState} to the global transaction table if it does not exist already.
    *
    * @return {@code true} if created, {@code false} otherwise.
    */
   boolean addGlobalState(XidImpl xid, TxState txState) {
      boolean created = globalTxTable.putIfAbsent(new CacheXid(cacheName, xid), txState) == null;
      if (trace && created) {
         log.tracef("[%s] New state=%s", xid, txState);
      }
      return created;
   }

   /**
    * Adds the {@link EmbeddedTransaction} in the local transaction table.
    */
   void createLocalTx(Xid xid, EmbeddedTransaction tx) {
      localTxTable.put(xid, tx);
      if (trace) {
         log.tracef("[%s] New tx=%s", xid, tx);
      }
   }

   /**
    * Removes the {@link TxState} from the global transaction table.
    */
   void removeGlobalState(XidImpl xid) {
      TxState state = globalTxTable.remove(new CacheXid(cacheName, xid));
      if (trace) {
         log.tracef("[%s] Removed state=%s", xid, state);
      }
   }

   /**
    * Updates the state in the global transaction table.
    *
    * @return {@code true} if the state is updated, {@code false} otherwise.
    */
   boolean updateGlobalState(XidImpl xid, TxState current, TxState update) {
      boolean updated = globalTxTable.replace(new CacheXid(cacheName, xid), current, update);
      if (trace && updated) {
         log.tracef("[%s] New state=%s (old state=%s)", xid, update, current);
      }
      return updated;
   }

   /**
    * @return The {@link TxState} associated to the {@code xid}.
    */
   TxState getGlobalState(XidImpl xid) {
      return globalTxTable.get(new CacheXid(cacheName, xid));
   }

   /**
    * testing only
    */
   boolean isEmpty() {
      return globalTxTable.isEmpty() && localTxTable.isEmpty();
   }
}
