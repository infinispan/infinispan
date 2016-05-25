package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.*;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupFilter;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Loads the entry into context but does not commit it.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class BaseEntryWrappingInterceptor extends DDAsyncInterceptor {
   protected static final EnumSet<Flag> IGNORE_OWNERSHIP_FLAGS =
      EnumSet.of(Flag.SKIP_OWNERSHIP_CHECK, Flag.CACHE_MODE_LOCAL);
   protected static final long IGNORE_OWNERSHIP_FLAG_BITS =
      EnumUtil.bitSetOf(Flag.SKIP_OWNERSHIP_CHECK, Flag.CACHE_MODE_LOCAL);
   protected static final Log log = LogFactory.getLog(BaseEntryWrappingInterceptor.class);
   protected static final boolean trace = log.isTraceEnabled();

   protected EntryFactory entryFactory;
   protected CacheNotifier notifier;
   protected ClusteringDependentLogic cdl;
   protected boolean isUsingLockDelegation;
   protected boolean isInvalidation;
   protected boolean isScattered;
   protected DataContainer<Object, Object> dataContainer;
   protected GroupManager groupManager;

   private final ReturnHandler dataReadReturnHandler = new ReturnHandler() {
      @Override
      public CompletableFuture<Object> handle(InvocationContext rCtx, VisitableCommand rCommand, Object rv,
                                              Throwable throwable) throws Throwable {
         AbstractDataCommand command1 = (AbstractDataCommand) rCommand;

         // Entry visit notifications used to happen in the CallInterceptor
         // We do it after (maybe) committing the entries, to avoid adding another try/finally block
         if (throwable == null && rv != null) {
            Object value = command1 instanceof GetCacheEntryCommand && rv instanceof CacheEntry ? ((CacheEntry) rv).getValue() : rv;
            notifier.notifyCacheEntryVisited(command1.getKey(), value, true, rCtx, command1);
            notifier.notifyCacheEntryVisited(command1.getKey(), value, false, rCtx, command1);
         }
         return null;
      }
   };

   @Inject
   public void init(EntryFactory entryFactory, DataContainer<Object, Object> dataContainer,
                    ClusteringDependentLogic cdl, GroupManager groupManager, CacheNotifier notifier) {
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
      this.cdl = cdl;
      this.groupManager = groupManager;
      this.notifier = notifier;
   }

   @Start
   public void start() {
      isUsingLockDelegation = !cacheConfiguration.transaction().transactionMode().isTransactional() &&
         (cacheConfiguration.clustering().cacheMode().isDistributed() ||
            cacheConfiguration.clustering().cacheMode().isReplicated());
      isInvalidation = cacheConfiguration.clustering().cacheMode().isInvalidation();
      isScattered = cacheConfiguration.clustering().cacheMode().isScattered();
   }


   @Override
   public final CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   @Override
   public final CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command);
   }

   protected abstract CompletableFuture<Void> handleDataWriteReturn(InvocationContext ctx, DataWriteCommand command) throws Throwable;

   protected abstract CompletableFuture<Void> handleManyWriteReturn(InvocationContext ctx) throws Throwable;

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      entryFactory.wrapEntryForDelta(ctx, command.getKey(), command.getDelta());
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      command.addFlags(IGNORE_OWNERSHIP_FLAGS); //to force the wrapping
      return visitRemoveCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      if (!command.isGroupOwner()) {
         return ctx.continueInvocation();
      }
      final KeyFilter<Object> keyFilter = new CompositeKeyFilter<>(new GroupFilter<>(groupName, groupManager),
                                                                   new CollectionKeyFilter<>(ctx.getLookedUpEntries().keySet()));
      dataContainer.executeTask(keyFilter, (o, internalCacheEntry) -> {
         // ignore tombstones
         if (internalCacheEntry.getValue() == null) {
            return;
         }
         synchronized (ctx) {
            //the process can be made in multiple threads, so we need to synchronize in the context.
            entryFactory.wrapExternalEntry(ctx, internalCacheEntry.getKey(), internalCacheEntry,
                                           EntryFactory.Wrap.STORE, false);
         }
      });
      return ctx.continueInvocation();
   }

   protected CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      ctx.onReturn(dataReadReturnHandler);
      entryFactory.wrapEntryForReading(ctx, command.getKey(), null, command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK));
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, null, command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK));
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         GetAllCommand getAllCommand = (GetAllCommand) rCommand;

         // Entry visit notifications used to happen in the CallInterceptor
         if (throwable == null && rv != null) {
            log.tracef("Notifying getAll? %s; result %s", !command.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION), rv);
            Map<Object, Object> map = (Map<Object, Object>) rv;
            // TODO: it would be nice to know if a listener was registered for this and
            // not do the full iteration if there was no visitor listener registered
            if (!command.hasFlag(Flag.SKIP_LISTENER_NOTIFICATION)) {
               for (Map.Entry<Object, Object> entry : map.entrySet()) {
                  Object value = entry.getValue();
                  if (value != null) {
                     value = command.isReturnEntries() ? ((CacheEntry) value).getValue() : entry.getValue();
                     notifier.notifyCacheEntryVisited(entry.getKey(), value, true, rCtx, getAllCommand);
                     notifier.notifyCacheEntryVisited(entry.getKey(), value, false, rCtx, getAllCommand);
                  }
               }
            }
         }
         return null;
      });
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      wrapEntryForRemoveIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      wrapEntryForReplaceIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
      for (Object key : command.getMap().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, ignoreOwnership);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      CacheEntry entry = entryFactory.wrapEntryForReading(ctx, command.getKey(), null, false);
      // Null entry is often considered to mean that entry is not available
      // locally, but if there's no need to get remote, the read-only
      // function needs to be executed, so force a non-null entry in
      // context with null content
      if (entry == null && cdl.localNodeIsOwner(command.getKey())) {
         entryFactory.wrapEntryForReading(ctx, command.getKey(), NullCacheEntry.getInstance(), false);
      }
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      for (Object key : command.getKeys()) {
         entryFactory.wrapEntryForReading(ctx, key, null, false);
      }
      return ctx.continueInvocation();
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
      for (Object key : command.getEntries().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, ignoreOwnership);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
      for (Object key : command.getKeys()) {
         if (shouldWrap(key, ctx, command)) {
            //the put map never reads the keys
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, true, ignoreOwnership);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      wrapEntryForPutIfNeeded(ctx, command);
      return handleDataWriteReturn(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
      for (Object key : command.getKeys()) {
         if (shouldWrap(key, ctx, command)) {
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, false, ignoreOwnership);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
      for (Object key : command.getEntries().keySet()) {
         if (shouldWrap(key, ctx, command)) {
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_ALL, false, ignoreOwnership);
         }
      }
      return handleManyWriteReturn(ctx);
   }

   private void wrapEntryForPutIfNeeded(InvocationContext ctx, AbstractDataWriteCommand command) throws Throwable {
      if (shouldWrap(command.getKey(), ctx, command)) {
         boolean skipRead = !command.readsExistingValues();
         boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), EntryFactory.Wrap.WRAP_ALL, skipRead, ignoreOwnership);
      }
   }

   private boolean shouldWrap(Object key, InvocationContext ctx, FlagAffectedCommand command) {
      if (command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK)) {
         if (trace)
            log.tracef("Skipping ownership check and wrapping key %s", toStr(key));

         return true;
      } else if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) {
            log.tracef("CACHE_MODE_LOCAL is set. Wrapping key %s", toStr(key));
         }
         return true;
      }
      boolean result;
      boolean isTransactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      boolean isPutForExternalRead = command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ);

      // Invalidated caches should always wrap entries in order to local
      // changes from nodes that are not lock owners for these entries.
      // Switching ClusteringDependentLogic to handle this, i.e.
      // localNodeIsPrimaryOwner to always return true, would have had negative
      // impact on locking since locks would be always be acquired locally
      // and that would lead to deadlocks.
      if (isInvalidation || (isTransactional && !isPutForExternalRead)) {
         result = true;
      } else {
         if (isUsingLockDelegation || isTransactional) {
            boolean isOwner = cdl.localNodeIsOwner(key);
            boolean isPrimaryOwner = cdl.localNodeIsPrimaryOwner(key);
            boolean originLocal = ctx.isOriginLocal();
            if (trace) {
               log.tracef("isPrimary=%s,isOwner=%s,originalLocal=%s", isPrimaryOwner, isOwner, originLocal);
            }
            result = originLocal ? isPrimaryOwner : isOwner;
         } else {
            result = cdl.canWriteEntry(key);
         }
      }

      if (trace)
         log.tracef("Wrapping entry '%s'? %s", toStr(key), result);

      return result;
   }

   private void wrapEntryForRemoveIfNeeded(InvocationContext ctx, RemoveCommand command) throws InterruptedException {
      if (shouldWrap(command.getKey(), ctx, command)) {
         boolean forceWrap = isScattered || command.getValueMatcher().nonExistentEntryCanMatch();
         EntryFactory.Wrap wrap = forceWrap ? EntryFactory.Wrap.WRAP_ALL : EntryFactory.Wrap.WRAP_NON_NULL;
         boolean skipRead = !command.readsExistingValues();
         boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, skipRead, ignoreOwnership);
      }
   }

   private void wrapEntryForReplaceIfNeeded(InvocationContext ctx, ReplaceCommand command) throws InterruptedException {
      if (shouldWrap(command.getKey(), ctx, command)) {
         // When retrying, we might still need to perform the command even if the previous value was removed
         // In scattered mode we wrap the entry even if it's not present locally
         EntryFactory.Wrap wrap =
            isScattered || command.getValueMatcher().nonExistentEntryCanMatch() ? EntryFactory.Wrap.WRAP_ALL :
               EntryFactory.Wrap.WRAP_NON_NULL;
         boolean skipRead = !command.readsExistingValues();
         boolean ignoreOwnership = (command.getFlagsBitSet() & IGNORE_OWNERSHIP_FLAG_BITS) != 0;
         entryFactory.wrapEntryForWriting(ctx, command.getKey(), wrap, skipRead, ignoreOwnership);
      }
   }
}
