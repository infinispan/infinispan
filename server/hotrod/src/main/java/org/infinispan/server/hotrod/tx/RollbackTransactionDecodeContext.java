package org.infinispan.server.hotrod.tx;

import java.util.Objects;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.command.tx.ForwardRollbackCommand;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ByteString;

/**
 * A decode context to handle rollback request from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class RollbackTransactionDecodeContext extends SecondPhaseTransactionDecodeContext {

   private static final Log log = LogFactory.getLog(RollbackTransactionDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   public RollbackTransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      super(cache, xid);
   }

   @Override
   final Log log() {
      return log;
   }

   @Override
   final boolean isTrace() {
      return trace;
   }

   @Override
   protected void performRemote() {
      if (txState.status() != Status.STATUS_ROLLEDBACK) {
         advance(txState.rollback());
      }
      rollbackRemoteTransaction();
   }

   @Override
   protected void performLocal() throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
      try {
         //local transaction
         EmbeddedTransaction tx = Objects.requireNonNull(serverTransactionTable.getLocalTx(xid));
         if (txState.status() != Status.STATUS_ROLLEDBACK) {
            advance(txState.rollback());
         }
         tx.runCommit(true);
      } catch (RollbackException ignored) {
         //ignore. rollback exception is always thrown here and we want to XA_OK code back to the client since it is a rollback request.
      } finally {
         serverTransactionTable.removeGlobalStateAndLocalTx(xid);
      }
   }

   @Override
   protected CacheRpcCommand buildForwardCommand(ByteString cacheName) {
      return new ForwardRollbackCommand(cacheName, XidImpl.copy(xid));
   }

}
