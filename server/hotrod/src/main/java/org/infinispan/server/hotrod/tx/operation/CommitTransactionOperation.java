package org.infinispan.server.hotrod.tx.operation;

import static org.infinispan.server.hotrod.tx.operation.Util.commitLocalTransaction;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import javax.security.auth.Subject;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import jakarta.transaction.HeuristicCommitException;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.command.tx.ForwardCommitCommand;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;

/**
 * It commits a transaction in all involved caches.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class CommitTransactionOperation extends BaseCompleteTransactionOperation<TransactionBoundaryCommand, ForwardCommitCommand> {

   private static final Log log = Log.getLog(CommitTransactionOperation.class);

   //TODO check if this class can implement the BiFunction interface!
   private final BiFunction<?, Throwable, Void> handler = (ignored, throwable) -> {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Handle response: value=%s, throwable=%s", xid, ignored, throwable);
      }
      if (throwable != null) {
         while (throwable != null) {
            if (throwable instanceof HeuristicCommitException) {
               hasCommits = true;
               return null;
            } else if (throwable instanceof HeuristicMixedException) {
               hasCommits = true;
               hasRollbacks = true;
               return null;
            } else if (throwable instanceof HeuristicRollbackException || throwable instanceof RollbackException) {
               hasRollbacks = true;
               return null;
            }
            throwable = throwable.getCause();
            //TODO handle suspect/outdated topology exceptions
         }
         hasErrors = true;
      } else {
         hasCommits = true;
      }
      return null;
   };

   public CommitTransactionOperation(HotRodHeader header, HotRodServer server, Subject subject, XidImpl xid,
         BiConsumer<HotRodHeader, Integer> reply) {
      super(header, server, subject, xid, reply);
   }

   @Override
   public void run() {
      globalTxTable.markToCommit(xid, this);
   }

   @Override
   public void addCache(ByteString cacheName, Status status) {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Collected cache %s status %s", xid, cacheName, status);
      }
      switch (status) {
         case MARK_ROLLBACK:
         case ROLLED_BACK:
            hasRollbacks = true; //this cache decided to rollback. we can't change its state.
            break;
         case COMMITTED:
            hasCommits = true; //this cache is already committed.
            break;
         case ERROR:
            hasErrors = true; //we failed to update this cache
            break;
         case NO_TRANSACTION:
         case PREPARING:
         case ACTIVE:
         case PREPARED:
            //this shouldn't happen!
            hasErrors = true;
            break;
         case MARK_COMMIT:
         case OK:
         default:
            cacheNames.add(cacheName); //this cache is ready to commit!
            break;
      }

      notifyCacheCollected();
   }

   @Override
   <T> BiFunction<T, Throwable, Void> handler() {
      return (BiFunction<T, Throwable, Void>) handler;
   }

   @Override
   void sendReply() {
      int xaCode = XAResource.XA_OK;
      if (hasErrors) {
         xaCode = XAException.XAER_RMERR;
      } else if (hasCommits && hasRollbacks) {
         xaCode = XAException.XA_HEURMIX;
      } else if (hasRollbacks) {
         //only rollbacks
         xaCode = XAException.XA_HEURRB;
      }
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Sending reply %s", xid, xaCode);
      }
      reply.accept(header, xaCode);
   }

   @Override
   TransactionBoundaryCommand buildRemoteCommand(Configuration configuration, CommandsFactory commandsFactory, TxState state) {
      if (configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC) {
         //pessimistic locking commits in 1PC
         return commandsFactory.buildPrepareCommand(state.getGlobalTransaction(), state.getModifications(), true);
      } else if (Configurations.isTxVersioned(configuration)) {
         //TODO we don't support versioning yet (only used for optimistic tx). When we do, we need to store the versions in TxState.
         return commandsFactory.buildVersionedCommitCommand(state.getGlobalTransaction());
      } else {
         return commandsFactory.buildCommitCommand(state.getGlobalTransaction());
      }
   }

   @Override
   ForwardCommitCommand buildForwardCommand(ByteString cacheName, long timeout) {
      return new ForwardCommitCommand(cacheName, xid, timeout);
   }

   @Override
   void asyncCompleteLocalTransaction(AdvancedCache<?, ?> cache, long timeout, AggregateCompletionStage<Void> stageCollector) {
      stageCollector.dependsOn(blockingManager.runBlocking(() -> {
         try {
            commitLocalTransaction(cache, xid, timeout);
         } catch (HeuristicMixedException e) {
            hasCommits = true;
            hasRollbacks = true;
         } catch (RollbackException | HeuristicRollbackException e) {
            hasRollbacks = true;
         } catch (Throwable t) {
            hasErrors = true;
         }
      }, this));
   }

}
