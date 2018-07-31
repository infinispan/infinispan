package org.infinispan.server.hotrod.tx.table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.transaction.tm.EmbeddedTransaction;

/**
 * A Transaction Table for client transaction.
 * <p>
 * It stores the global state of a transaction and the map between the {@link Xid} and {@link Transaction}'s run
 * locally.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class PerCacheTxTable {

   private static final Log log = LogFactory.getLog(PerCacheTxTable.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<Xid, EmbeddedTransaction> localTxTable = new ConcurrentHashMap<>();
   private final ClientAddress clientAddress;


   public PerCacheTxTable(Address address) {
      this.clientAddress = new ClientAddress(address);
   }

   public ClientAddress getClientAddress() {
      return clientAddress;
   }

   /**
    * @return The local {@link EmbeddedTransaction} associated to the {@code xid}.
    */
   public EmbeddedTransaction getLocalTx(Xid xid) {
      return localTxTable.get(xid);
   }

   /**
    * Removes the local {@link EmbeddedTransaction} associated to {@code xid}.
    */
   public void removeLocalTx(Xid xid) {
      EmbeddedTransaction tx = localTxTable.remove(xid);
      if (trace) {
         log.tracef("[%s] Removed tx=%s", xid, tx);
      }
   }

   /**
    * Adds the {@link EmbeddedTransaction} in the local transaction table.
    */
   public void createLocalTx(Xid xid, EmbeddedTransaction tx) {
      localTxTable.put(xid, tx);
      if (trace) {
         log.tracef("[%s] New tx=%s", xid, tx);
      }
   }

   /**
    * testing only
    */
   public boolean isEmpty() {
      if (trace) {
         log.tracef("Active Transactions: %s", localTxTable.keySet());
      }
      return localTxTable.isEmpty();
   }
}
