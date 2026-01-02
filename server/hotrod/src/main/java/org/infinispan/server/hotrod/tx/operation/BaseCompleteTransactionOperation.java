package org.infinispan.server.hotrod.tx.operation;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.validOnly;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import javax.security.auth.Subject;
import javax.transaction.xa.XAException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.CacheNameCollector;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * A base class to complete a transaction (commit or rollback).
 * <p>
 * This class implements {@link Runnable} in order to be executed in a {@link ExecutorService}.
 * <p>
 * A transaction completion occurs in the following steps:
 *
 * <ul>
 * <li>Search and collects all the cache involved in the transaction</li>
 * <li>Finds the transaction originator (i.e. the node that replayed the transaction)</li>
 * <li>If it is an originator of one or more caches, it completes the transaction in another thread</li>
 * <li>If the originator is not in the topology, it completes the transaction by broadcasting the respective command</li>
 * <li>If the originator is in the topology, it forwards the completion request</li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
abstract class BaseCompleteTransactionOperation<C1 extends TransactionBoundaryCommand, C2 extends CacheRpcCommand> implements CacheNameCollector, Runnable {

   private static final Log log = Log.getLog(BaseCompleteTransactionOperation.class);

   final XidImpl xid;
   final GlobalTxTable globalTxTable;
   final HotRodHeader header;
   final BiConsumer<HotRodHeader, Integer> reply;
   private final HotRodServer server;
   private final Subject subject;
   final Collection<ByteString> cacheNames = new ConcurrentLinkedQueue<>();
   final BlockingManager blockingManager;
   private final AtomicInteger expectedCaches = new AtomicInteger();
   volatile boolean hasErrors = false;
   volatile boolean hasCommits = false;
   volatile boolean hasRollbacks = false;

   BaseCompleteTransactionOperation(HotRodHeader header, HotRodServer server, Subject subject, XidImpl xid,
         BiConsumer<HotRodHeader, Integer> reply) {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(server.getCacheManager());
      globalTxTable = gcr.getComponent(GlobalTxTable.class);
      blockingManager = gcr.getComponent(BlockingManager.class);
      this.header = header;
      this.server = server;
      this.subject = subject;
      this.xid = xid;
      this.reply = reply;
   }

   @Override
   public final void expectedSize(int size) {
      expectedCaches.set(size);
   }

   @Override
   public final void noTransactionFound() {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] No caches found.", xid);
      }
      //no transactions
      reply.accept(header, XAException.XAER_NOTA);
   }

   /**
    * It returns the handler to handle the replies from remote nodes or from a forward request.
    * <p>
    * It is invoked when this node isn't the originator for a cache. If the originator isn't in the view, it handles the
    * replies from the broadcast commit or rollback command. If the originator is in the view, it handles the reply from
    * the forward command.
    *
    * @return The {@link BiFunction} to handle the remote replies.
    */
   abstract <T> BiFunction<T, Throwable, Void> handler();

   /**
    * When all caches are completed, this method is invoked to reply to the Hot Rod client.
    */
   abstract void sendReply();

   /**
    * When the originator is not in the cache topology, this method builds the command to broadcast to all the nodes.
    *
    * @return The completion command to broadcast to nodes in the cluster.
    */
   abstract C1 buildRemoteCommand(Configuration configuration, CommandsFactory commandsFactory,
         TxState state);

   /**
    * When this node isn't the originator, it builds the forward command to send to the originator.
    *
    * @return The forward command to send to the originator.
    */
   abstract C2 buildForwardCommand(ByteString cacheName, long timeout);

   /**
    * When this node is the originator, this method is invoked to complete the transaction in the specific cache.
    */
   abstract void asyncCompleteLocalTransaction(AdvancedCache<?, ?> cache, long timeout, AggregateCompletionStage<Void> stageCollector);

   /**
    * Invoked every time a cache is found to be involved in a transaction.
    */
   void notifyCacheCollected() {
      int result = expectedCaches.decrementAndGet();
      if (log.isTraceEnabled()) {
         log.tracef("[%s] Cache collected. Missing=%s.", xid, result);
      }
      if (result == 0) {
         onCachesCollected();
      }
   }

   /**
    * Invoked when all caches are ready to complete the transaction.
    */
   private void onCachesCollected() {
      if (log.isTraceEnabled()) {
         log.tracef("[%s] All caches collected: %s", xid, cacheNames);
      }
      int size = cacheNames.size();
      if (size == 0) {
         //it can happen if all caches either commit or thrown an exception
         sendReply();
         return;
      }

      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      for (ByteString cacheName : cacheNames) {
         try {
            completeCache(cacheName, aggregateCompletionStage);
         } catch (Throwable t) {
            if (log.isTraceEnabled()) {
               log.tracef(t, "[%s] Error while trying to complete transaction for cache %s", xid, cacheName);
            }
            hasErrors = true;
         }
      }
      aggregateCompletionStage.freeze().thenRun(this::sendReply);
   }

   /**
    * Completes the transaction for a specific cache.
    */
   private void completeCache(ByteString cacheName, AggregateCompletionStage<Void> stageCollector) throws Throwable {
      TxState state = globalTxTable.getState(new CacheXid(cacheName, xid));
      HotRodServer.ExtendedCacheInfo cacheInfo =
            server.getCacheInfo(cacheName.toString(), header.getVersion(), header.getMessageId(), true);
      AdvancedCache<?, ?> cache = server.cache(cacheInfo, header, subject);
      var distributionManager = cache.getDistributionManager();
      var topology = distributionManager == null ? null : distributionManager.getCacheTopology();
      if (topology == null || topology.getLocalAddress().equals(state.getOriginator())) {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Completing local executed transaction.", xid);
         }
         asyncCompleteLocalTransaction(cache, state.getTimeout(), stageCollector);
      } else if (topology.getMembers().contains(state.getOriginator())) {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Forward remotely executed transaction to %s.", xid, state.getOriginator());
         }
         forwardCompleteCommand(cacheName, state, cache.getRpcManager(), stageCollector);
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("[%s] Originator, %s, left the cluster.", xid, state.getOriginator());
         }
         completeWithRemoteCommand(cache, state, cache.getRpcManager(), topology.getTopologyId(), stageCollector);
      }
   }

   /**
    * Completes the transaction in the cache when the originator no longer belongs to the cache topology.
    */
   private void completeWithRemoteCommand(AdvancedCache<?, ?> cache, TxState state, RpcManager rpcManager,
                                          int topologyId, AggregateCompletionStage<Void> stageCollector) throws Throwable {
      var registry = SecurityActions.getCacheComponentRegistry(cache);
      var commandsFactory = registry.getCommandsFactory();
      var command = buildRemoteCommand(cache.getCacheConfiguration(), commandsFactory, state);
      command.setTopologyId(topologyId);
      stageCollector.dependsOn(rpcManager.invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions())
            .handle(handler()));
      stageCollector.dependsOn(command.invokeAsync(registry).handle(handler()));
   }

   /**
    * Completes the transaction in the cache when the originator is still in the cache topology.
    */
   private void forwardCompleteCommand(ByteString cacheName, TxState state, RpcManager rpcManager, AggregateCompletionStage<Void> stageCollector) {
      //TODO what if the originator crashes in the meanwhile?
      //actually, the reaper would rollback the transaction later...
      var originator = state.getOriginator();
      var command = buildForwardCommand(cacheName, state.getTimeout());
      stageCollector.dependsOn(rpcManager.invokeCommand(originator, command, validOnly(), rpcManager.getSyncRpcOptions())
            .handle(handler()));
   }

}
