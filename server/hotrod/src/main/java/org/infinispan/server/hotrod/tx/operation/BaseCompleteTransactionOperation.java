package org.infinispan.server.hotrod.tx.operation;

import static org.infinispan.remoting.transport.impl.VoidResponseCollector.validOnly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.HotRodHeader;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.table.CacheNameCollector;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.util.ByteString;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
abstract class BaseCompleteTransactionOperation implements CacheNameCollector, Runnable {

   final XidImpl xid;
   final GlobalTxTable globalTxTable;
   final HotRodHeader header;
   final BiConsumer<HotRodHeader, Integer> reply;
   private final HotRodServer server;
   private final Subject subject;
   final Collection<ByteString> cacheNames = new ConcurrentLinkedQueue<>();
   final ExecutorService asyncExecutor;
   private final AtomicInteger expectedCaches = new AtomicInteger();
   volatile boolean hasErrors = false;
   volatile boolean hasCommits = false;
   volatile boolean hasRollbacks = false;

   BaseCompleteTransactionOperation(HotRodHeader header, HotRodServer server, Subject subject, XidImpl xid,
         BiConsumer<HotRodHeader, Integer> reply) {
      GlobalComponentRegistry gcr = server.getCacheManager().getGlobalComponentRegistry();
      this.globalTxTable = gcr.getComponent(GlobalTxTable.class);
      this.asyncExecutor = gcr.getComponent(ExecutorService.class, KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR);
      this.header = header;
      this.server = server;
      this.subject = subject;
      this.xid = xid;
      this.reply = reply;
   }

   @Override
   public final void expectedSize(int size) {
      this.expectedCaches.set(size);
   }

   @Override
   public final void noTransactionFound() {
      if (isTraceEnabled()) {
         log().tracef("[%s] No caches found.", xid);
      }
      //no transactions
      reply.accept(header, XAException.XAER_NOTA);
   }

   abstract <T> BiFunction<T, Throwable, Void> handler();

   abstract void sendReply();

   abstract CacheRpcCommand buildRemoteCommand(Configuration configuration, CommandsFactory commandsFactory, TxState state);

   abstract CacheRpcCommand buildForwardCommand(ByteString cacheName, long timeout);

   abstract CompletableFuture<Void> asyncCompleteLocalTransaction(AdvancedCache<?, ?> cache, long timeout);

   abstract Log log();

   abstract boolean isTraceEnabled();

   void notifyCacheCollected() {
      int result = expectedCaches.decrementAndGet();
      if (isTraceEnabled()) {
         log().tracef("[%s] Cache collected. Missing=%s.", xid, result);
      }
      if (result == 0) {
         onCachesCollected();
      }
   }

   private void onCachesCollected() {
      if (isTraceEnabled()) {
         log().tracef("[%s] All caches collected: %s", xid, cacheNames);
      }
      int size = cacheNames.size();
      if (size == 0) {
         //it can happen if all caches either commit or thrown an exception
         sendReply();
         return;
      }

      List<CompletableFuture<Void>> futures = new ArrayList<>(size);
      for (ByteString cacheName : cacheNames) {
         try {
            futures.add(completeCache(cacheName));
         } catch (Throwable t) {
            hasErrors = true;
         }
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenRun(this::sendReply);
   }

   private CompletableFuture<Void> completeCache(ByteString cacheName) throws Throwable {
      TxState state = globalTxTable.getState(new CacheXid(cacheName, xid));
      AdvancedCache<?, ?> cache = server.cache(header, subject, cacheName.toString());
      RpcManager rpcManager = cache.getRpcManager();
      if (rpcManager == null || rpcManager.getAddress().equals(state.getOriginator())) {
         if (isTraceEnabled()) {
            log().tracef("[%s] Completing local executed transaction.", xid);
         }
         return asyncCompleteLocalTransaction(cache, state.getTimeout());
      } else if (rpcManager.getMembers().contains(state.getOriginator())) {
         if (isTraceEnabled()) {
            log().tracef("[%s] Forward remotely executed transaction to %s.", xid, state.getOriginator());
         }
         return forwardCompleteCommand(cacheName, rpcManager, state);
      } else {
         if (isTraceEnabled()) {
            log().tracef("[%s] Originator, %s, left the cluster.", xid, state.getOriginator());
         }
         return completeWithRemoteCommand(cache, rpcManager, state);
      }
   }

   private CompletableFuture<Void> completeWithRemoteCommand(AdvancedCache<?, ?> cache, RpcManager rpcManager, TxState state)
         throws Throwable {
      CommandsFactory commandsFactory = cache.getComponentRegistry().getCommandsFactory();
      CacheRpcCommand command = buildRemoteCommand(cache.getCacheConfiguration(), commandsFactory, state);
      CompletableFuture<Void> remote = rpcManager
            .invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions())
            .handle(handler())
            .toCompletableFuture();
      commandsFactory.initializeReplicableCommand(command, false);
      CompletableFuture<Void> local = command.invokeAsync().handle(handler());
      return CompletableFuture.allOf(remote, local);
   }

   private CompletableFuture<Void> forwardCompleteCommand(ByteString cacheName, RpcManager rpcManager,
         TxState state) {
      //TODO what if the originator crashes in the meanwhile?
      //actually, the reaper would rollback the transaction later...
      Address originator = state.getOriginator();
      CacheRpcCommand command = buildForwardCommand(cacheName, state.getTimeout());
      return rpcManager.invokeCommand(originator, command, validOnly(), rpcManager.getSyncRpcOptions())
            .handle(handler())
            .toCompletableFuture();
   }

}
