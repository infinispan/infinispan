package org.infinispan.server.hotrod.tx;

import java.util.Collections;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.ByteString;

/**
 * A base decode context to handle rollback or commit request from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public abstract class SecondPhaseTransactionDecodeContext extends TransactionDecodeContext {

   private final ByteString cacheName;

   SecondPhaseTransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      super(cache, xid);
      this.cacheName = ByteString.fromString(cache.getName());
   }

   /**
    * Commits or Rollbacks the transaction.
    *
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    */
   public final void perform() throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
      if (txState == null) {
         if (isTrace()) {
            log().tracef("Transaction with XID=%s already finished.", xid);
         }
         //already committed?
         serverTransactionTable.removeLocalTx(xid);
         return;
      }
      final Address originator = txState.getOriginator();
      if (isLocalMode() || localAddress.equals(originator)) {
         performLocal();
      } else if (isAlive(originator)) {
         forwardToOriginator();
      } else {
         performRemote();
      }
   }

   abstract Log log();

   abstract boolean isTrace();

   /**
    * Commits or rollbacks the transaction as a remote transaction since the member who executed the transaction left
    * the cluster
    */
   protected abstract void performRemote();

   /**
    * Commits or rollbacks a local transaction.
    *
    * @throws HeuristicMixedException    If a heuristic decision was made and some some parts of the transaction have
    *                                    been committed while other parts have been rolled back.
    * @throws HeuristicRollbackException If a heuristic decision to roll back the transaction was made.
    * @throws RollbackException          If the transaction was marked for rollback only, the transaction is rolled back
    *                                    and this exception is thrown.
    */
   protected abstract void performLocal() throws HeuristicRollbackException, HeuristicMixedException, RollbackException;

   /**
    * Creates a forward commit or rollback commands to send to the member who executed the transaction.
    */
   protected abstract CacheRpcCommand buildForwardCommand(ByteString cacheName);

   private void forwardToOriginator() {
      CacheRpcCommand forwardCommand = buildForwardCommand(cacheName);
      rpcManager.invokeRemotely(Collections.singleton(txState.getOriginator()), forwardCommand,
            rpcManager.getDefaultRpcOptions(true));
   }

}
