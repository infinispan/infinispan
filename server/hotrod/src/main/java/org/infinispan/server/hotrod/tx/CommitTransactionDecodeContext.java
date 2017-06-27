package org.infinispan.server.hotrod.tx;

import java.util.Objects;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.command.tx.ForwardCommitCommand;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ByteString;

/**
 * A decode context that handle a commit request from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class CommitTransactionDecodeContext extends SecondPhaseTransactionDecodeContext {

   private static final Log log = LogFactory.getLog(CommitTransactionDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final boolean onePhaseCommit;

   public CommitTransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      super(cache, xid);
      this.onePhaseCommit = cache.getCacheConfiguration().transaction().lockingMode() == LockingMode.PESSIMISTIC;
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
      try {
         if (onePhaseCommit) {
            onePhaseCommit();
         } else {
            secondPhaseCommit();
         }
      } catch (Throwable throwable) {
         throw Util.rewrapAsCacheException(throwable);
      } finally {
         forgetTransaction();
      }
   }

   @Override
   protected void performLocal() throws HeuristicRollbackException, HeuristicMixedException, RollbackException {
      try {
         //local transaction
         EmbeddedTransaction tx = Objects.requireNonNull(serverTransactionTable.getLocalTx(xid));
         if (txState.status() != Status.STATUS_COMMITTED) {
            updateState();
         }
         tx.runCommit(false);
      } finally {
         serverTransactionTable.removeGlobalStateAndLocalTx(xid);
      }
   }

   @Override
   protected CacheRpcCommand buildForwardCommand(ByteString cacheName) {
      return new ForwardCommitCommand(cacheName, XidImpl.copy(xid));
   }

   private void onePhaseCommit() throws Throwable {
      PrepareCommand command = commandsFactory.buildPrepareCommand(txState.getGlobalTransaction(),
            txState.getModifications(), true);
      updateState();
      rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(true));
      commandsFactory.initializeReplicableCommand(command, false);
      command.invokeAsync().join();
   }

   private void secondPhaseCommit() throws Throwable {
      CommitCommand command = commandsFactory.buildCommitCommand(txState.getGlobalTransaction());
      updateState();
      rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(true));
      commandsFactory.initializeReplicableCommand(command, false);
      command.invokeAsync().join();
   }

   private void updateState() {
      if (txState.status() != Status.STATUS_COMMITTED) {
         advance(txState.commit());
      }
   }

}
