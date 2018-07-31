package org.infinispan.server.hotrod.tx.operation;

import static org.infinispan.server.hotrod.tx.operation.Util.rollbackLocalTransaction;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import javax.security.auth.Subject;
import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.command.tx.ForwardRollbackCommand;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class RollbackTransactionOperation extends BaseCompleteTransactionOperation {

   private static final Log log = LogFactory.getLog(RollbackTransactionOperation.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final BiFunction<?, Throwable, Void> handler = (ignored, throwable) -> {
      if (throwable != null) {
         while (throwable != null) {
            if (throwable instanceof HeuristicRollbackException || throwable instanceof RollbackException) {
               hasRollbacks = true;
               return null;
            } else if (throwable instanceof HeuristicCommitException) {
               hasCommits = true;
            } else if (throwable instanceof HeuristicMixedException) {
               hasCommits = true;
               hasRollbacks = true;
               return null;
            }
            throwable = throwable.getCause();
         }
         //any other exception will fit here.
         hasErrors = true;
      } else {
         hasRollbacks = true;
      }
      return null;
   };

   public RollbackTransactionOperation(HotRodHeader header, HotRodServer server, Subject subject, XidImpl xid,
         BiConsumer<HotRodHeader, Integer> reply) {
      super(header, server, subject, xid, reply);
   }

   @Override
   public void run() {
      globalTxTable.markToRollback(xid, this);
   }

   @Override
   public void addCache(ByteString cacheName, Status status) {
      if (trace) {
         log.tracef("[%s] Collected cache %s status %s", xid, cacheName, status);
      }
      switch (status) {
         case ROLLED_BACK:
            break;
         case MARK_COMMIT:
         case COMMITTED:
            hasCommits = true; //this cache decided to commit?
            break;
         case ERROR:
            hasErrors = true; //we failed to update this cache
            break;
         case PREPARED:
         case ACTIVE:
         case PREPARING:
         case NO_TRANSACTION:
            //TODO this should happen!
            hasErrors = true;
            break;
         case OK:
         case MARK_ROLLBACK:
         default:
            cacheNames.add(cacheName); //ready to rollback.
            break;
      }

      notifyCacheCollected();
   }

   @Override
   <T> BiFunction<T, Throwable, Void> handler() {
      //noinspection unchecked
      return (BiFunction<T, Throwable, Void>) handler;
   }

   @Override
   void sendReply() {
      int xaCode = XAResource.XA_OK;
      if (hasErrors) {
         //exceptions...
         xaCode = XAException.XAER_RMERR;
      } else if (hasRollbacks && hasCommits) {
         //some caches decide to commit and others decide to rollback.
         xaCode = XAException.XA_HEURMIX;
      } else if (hasCommits) {
         //all caches involved decided to commit
         xaCode = XAException.XA_HEURCOM;
      }
      if (trace) {
         log.tracef("[%s] Sending reply %s", xid, xaCode);
      }
      reply.accept(header, xaCode);
   }

   @Override
   CacheRpcCommand buildRemoteCommand(Configuration configuration, CommandsFactory commandsFactory, TxState state) {
      return commandsFactory.buildRollbackCommand(state.getGlobalTransaction());
   }

   @Override
   CacheRpcCommand buildForwardCommand(ByteString cacheName, long timeout) {
      return new ForwardRollbackCommand(cacheName, xid, timeout);
   }

   @Override
   CompletableFuture<Void> asyncCompleteLocalTransaction(AdvancedCache<?, ?> cache, long timeout) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      asyncExecutor.submit(() -> {
         try {
            rollbackLocalTransaction(cache, xid, timeout);
         } catch (HeuristicMixedException e) {
            hasCommits = true;
            hasRollbacks = true;
         } catch (Throwable t) {
            hasErrors = true;
         }
         cf.complete(null);
      });
      return cf;
   }

   @Override
   Log log() {
      return log;
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

}
