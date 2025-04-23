package org.infinispan.server.hotrod.tx.table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.transaction.tm.EmbeddedTransaction;

import jakarta.transaction.Transaction;

/**
 * A Transaction Table for client transaction.
 * <p>
 * It stores the global state of a transaction and the map between the {@link XidImpl} and {@link Transaction}'s run
 * locally.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
//TODO merge with org.infinispan.server.hotrod.tx.table.GlobalTxTable
public class PerCacheTxTable {

   private static final Log log = LogFactory.getLog(PerCacheTxTable.class, Log.class);
   private final Map<XidImpl, EmbeddedTransaction> localTxTable = new ConcurrentHashMap<>();

   /**
    * @return The local {@link EmbeddedTransaction} associated to the {@code xid}.
    */
   public EmbeddedTransaction getLocalTx(XidImpl xid) {
      return localTxTable.get(xid);
   }

   /**
    * Removes the local {@link EmbeddedTransaction} associated to {@code xid}.
    */
   public void removeLocalTx(XidImpl xid) {
      EmbeddedTransaction tx = localTxTable.remove(xid);
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Removed tx=%s", xid, tx);
      }
   }

   /**
    * Adds the {@link EmbeddedTransaction} in the local transaction table.
    */
   public void createLocalTx(XidImpl xid, EmbeddedTransaction tx) {
      localTxTable.put(xid, tx);
      if (log.isTraceEnabled()) {
         log.tracef("[%s] New tx=%s", xid, tx);
      }
   }

   /**
    * testing only
    */
   public boolean isEmpty() {
      if (log.isTraceEnabled()) {
         log.tracef("Active Transactions: %s", localTxTable.keySet());
      }
      return localTxTable.isEmpty();
   }
}
