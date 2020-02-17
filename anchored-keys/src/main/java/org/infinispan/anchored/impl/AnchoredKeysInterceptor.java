package org.infinispan.anchored.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;

import net.jcip.annotations.GuardedBy;

/**
 * @author Dan
 */
@Scope(Scopes.NAMED_CACHE)
@Listener
public class AnchoredKeysInterceptor extends DDAsyncInterceptor {
   public static final String ANCHOR_CACHE_PREFIX = "___anchor_";

   @Inject EmbeddedCacheManager manager;
   @Inject RpcManager rpcManager;
   @Inject Transport transport;
   @Inject CacheManagerNotifier notifier;
   @Inject
   @ComponentName(KnownComponentNames.CACHE_NAME)
   String cacheName;
   @Inject CommandsFactory commandsFactory;

   Cache<Object, Object> anchorCache;
   volatile Address currentWriter;
   @GuardedBy("this")
   int currentViewId = -1;

   public AnchoredKeysInterceptor(Cache<Object, Object> anchorCache) {
      this.anchorCache = anchorCache;
   }

   @Start
   public void start() {
      notifier.addListener(this);
      updateWriter(transport.getMembers(), transport.getViewId());
   }

   @ViewChanged
   public void onViewChange(ViewChangedEvent event) {
      updateWriter(event.getNewMembers(), event.getViewId());
   }

   private void updateWriter(List<Address> members, int viewId) {
      synchronized (this) {
         if (viewId > currentViewId) {
            currentViewId = viewId;
            currentWriter = members.get(members.size() - 1);
         }
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      if (!ctx.isOriginLocal() || !ctx.lookupEntry(key).isNull()) {
         // The local no
         return invokeNext(ctx, command);
      }

      // The key doesn't exist locally, look it up in the ownership cache
      Address writer = this.currentWriter;
      return computeKeyWriter(ctx, command, key, writer)
            .thenApply(ctx, command, (rCtx, rCommand, rv) -> {
               Address owner = rv != null ? (Address) rv : writer;
               if (owner == transport.getAddress()) {
                  // The key was assigned to this node, but it was removed
                  return invokeNext(ctx, command);
               }

               command.setTopologyId(rpcManager.getTopologyId());
               return asyncValue(invokeRemotely(owner, command));
            });
   }

   private InvocationStage computeKeyWriter(InvocationContext ctx, PutKeyValueCommand command, Object key,
                                            Address currentWriter) {
      return asyncValue(anchorCache.getAsync(key))
            .thenApplyMakeStage(ctx, command, (rCtx, rCommand, rv) -> {
               Address existingOwner = (Address) rv;
               if (existingOwner != null)
                  return existingOwner;

               Object key1 = rCommand.getKey();
               return asyncValue(anchorCache.putIfAbsentAsync(key1, currentWriter));
            });
   }

   private CompletionStage<Object> invokeRemotely(Address owner, ReplicableCommand command) {
      return rpcManager.invokeCommand(owner, command, new IgnoreLeaversCollector(),
                                      rpcManager.getSyncRpcOptions());
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return super.visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return super.visitReplaceCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return super.visitComputeIfAbsentCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return super.visitComputeCommand(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return super.visitClearCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return super.visitEvictCommand(ctx, command);
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      return super.visitSizeCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      if (!ctx.isOriginLocal() || !ctx.lookupEntry(key).isNull()) {
         return invokeNext(ctx, command);
      }

      // The key doesn't exist locally, look it up in the ownership cache
      CompletableFuture<Object> ownerStage = anchorCache.getAsync(key);
      return asyncValue(ownerStage).thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         Address owner = (Address) rv;
         if (owner == null) {
            // No owner => no value
            return null;
         }

         command.setTopologyId(rpcManager.getTopologyId());
         return asyncValue(invokeRemotely(owner, command));
      });
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return super.visitGetCacheEntryCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return super.visitGetAllCommand(ctx, command);
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      return super.visitKeySetCommand(ctx, command);
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      return super.visitEntrySetCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return super.visitPrepareCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return super.visitRollbackCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return super.visitCommitCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      return super.visitInvalidateCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      return super.visitInvalidateL1Command(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      return super.visitLockControlCommand(ctx, command);
   }

   @Override
   public Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return super.visitUnknownCommand(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      return super.visitGetKeysInGroupCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return super.visitReadOnlyKeyCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return super.visitReadOnlyManyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return super.visitWriteOnlyKeyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return super.visitReadWriteKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return super.visitReadWriteKeyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command)
         throws Throwable {
      return super.visitWriteOnlyManyEntriesCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return super.visitWriteOnlyKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return super.visitWriteOnlyManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return super.visitReadWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command)
         throws Throwable {
      return super.visitReadWriteManyEntriesCommand(ctx, command);
   }

   private static class IgnoreLeaversCollector extends ValidSingleResponseCollector<Object> {
      @Override
      protected Object withValidResponse(Address sender, ValidResponse response) {
         return response.getResponseValue();
      }

      @Override
      protected ValidResponse targetNotFound(Address sender) {
         return SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
      }
   }
}
