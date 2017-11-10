package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.interceptors.impl.MultiSubCommandInvoker;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that handles L1 logic for non-transactional caches.
 *
 * @author Mircea Markus
 * @author William Burns
 */
public class L1NonTxInterceptor extends BaseRpcInterceptor {

   private static final Log log = LogFactory.getLog(L1NonTxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject protected L1Manager l1Manager;
   @Inject protected ClusteringDependentLogic cdl;
   @Inject protected EntryFactory entryFactory;
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected DataContainer dataContainer;
   @Inject protected StateTransferLock stateTransferLock;

   private long l1Lifespan;
   private long replicationTimeout;

   /**
    *  This map holds all the current write synchronizers registered for a given key.  This map is only added to when an
    * operation is invoked that would cause a remote get to occur (which is controlled by whether or not the
    * {@link L1NonTxInterceptor#skipL1Lookup(FlagAffectedCommand, Object)} method returns
    * true.  This map <b>MUST</b> have the value inserted removed in a finally block after the remote get is done to
    * prevent reference leaks.
    * <p>
    * Having a value in this map allows for other concurrent operations that require a remote get to not have to
    * actually perform a remote get as the first thread is doing this.  So in this case any subsequent operations
    * wanting the remote value can just call the
    * {@link L1WriteSynchronizer#get()} method or one of it's overridden
    * methods.  Note the way to tell if another thread is performing the remote get is to use the
    * {@link ConcurrentMap#putIfAbsent(Object, Object)} method and check if the return value is null or not.
    * <p>
    * Having a value in this map allows for a concurrent write or L1 invalidation to try to stop the synchronizer from
    * updating the L1 value by invoking it's
    * {@link L1WriteSynchronizer#trySkipL1Update()} method.  If this method
    * returns false, then the write or L1 invalidation <b>MUST</b> wait for the synchronizer to complete before
    * continuing to ensure it is able to remove the newly cached L1 value as it is now invalid.  This waiting should be
    * done by calling {@link L1WriteSynchronizer#get()} method or one of it's
    * overridden methods.  Failure to wait for the update to occur could cause a L1 data inconsistency as the
    * invalidation may not invalidate the new value.
    */
   private final ConcurrentMap<Object, L1WriteSynchronizer> concurrentWrites = CollectionFactory.makeConcurrentMap();

   @Start
   public void start() {
      l1Lifespan = cacheConfiguration.clustering().l1().lifespan();
      replicationTimeout = cacheConfiguration.clustering().remoteTimeout();
   }

   @Override
   public final Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command, false);
   }

   @Override
   public final Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return visitDataReadCommand(ctx, command, true);
   }

   private Object visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command,
         boolean isEntry) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, isEntry, false, true);
   }

   protected Object performCommandWithL1WriteIfAble(InvocationContext ctx, DataCommand command,
         boolean isEntry, boolean shouldAlwaysRunNextInterceptor, boolean registerL1) throws Throwable {
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         // If the command isn't going to return a remote value - just pass it down the interceptor chain
         if (!skipL1Lookup(command, key)) {
            return performL1Lookup(ctx, command, shouldAlwaysRunNextInterceptor, key, isEntry);
         }
      } else {
         // If this is a remote command, and we found a value in our cache
         // we store it so that we can later invalidate it
         if (registerL1) {
            l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
         }
      }
      return invokeNext(ctx, command);
   }

   private Object performL1Lookup(InvocationContext ctx, VisitableCommand command,
                                                boolean runInterceptorOnConflict, Object key, boolean isEntry) throws Throwable {
      // Most times the putIfAbsent will be successful, so not doing a get first
      L1WriteSynchronizer l1WriteSync = new L1WriteSynchronizer(dataContainer, l1Lifespan, stateTransferLock,
                                                                cdl);
      L1WriteSynchronizer presentSync = concurrentWrites.putIfAbsent(key, l1WriteSync);

      // If the sync was null that means we are the first to register for the given key.  If not that means there is
      // a concurrent request that also wants to do a remote get for the key.  If there was another thread requesting
      // the key we should wait until they get the value instead of doing another remote get.
      if (presentSync == null) {
         // Note this is the same synchronizer we just created that is registered with the L1Manager
         l1Manager.registerL1WriteSynchronizer(key, l1WriteSync);
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
            if (t != null) {
               l1WriteSync.retrievalEncounteredException(t);
            }
            // TODO Do we need try/finally here?
            l1Manager.unregisterL1WriteSynchronizer(key, l1WriteSync);
            concurrentWrites.remove(key);
         });
      } else {
         if (trace) {
            log.tracef("Found current request for key %s, waiting for their invocation's response", key);
         }
         Object returnValue;
         try {
            returnValue = presentSync.get(replicationTimeout, TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            // This should never be required since the status is always set in a try catch above - but IBM
            // doesn't...
            log.warnf("Synchronizer didn't return in %s milliseconds - running command normally!",
                  replicationTimeout);
            // Always run next interceptor if a timeout occurs
            return invokeNext(ctx, command);
         } catch (ExecutionException e) {
            throw e.getCause();
         }
         if (runInterceptorOnConflict) {
            // The command needs to write something. Execute the rest of the invocation chain.
            return invokeNext(ctx, command);
         } else if (!isEntry && returnValue instanceof InternalCacheEntry) {
            // The command is read-only, and we found the value in the L1 cache. Return it.
            returnValue = ((InternalCacheEntry) returnValue).getValue();
         }
         return returnValue;
      }
   }

   protected boolean skipL1Lookup(FlagAffectedCommand command, Object key) {
      return command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)
            || command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) || cdl.getCacheTopology().isWriteOwner(key)
            || dataContainer.containsKey(key);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command);
   }

   private Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command) {
      Collection<?> keys = command.getAffectedKeys();
      Set<Object> toInvalidate = new HashSet<>(keys.size());
      for (Object k : keys) {
         if (cdl.getCacheTopology().isWriteOwner(k)) {
            toInvalidate.add(k);
         }
      }

      CompletableFuture<?> invalidationFuture =
            !toInvalidate.isEmpty() ? l1Manager.flushCache(toInvalidate, ctx.getOrigin(), true) : null;

      //we also need to remove from L1 the keys that are not ours
      Iterator<VisitableCommand> subCommands = keys.stream().filter(
            k -> !cdl.getCacheTopology().isWriteOwner(k)).map(k -> removeFromL1Command(ctx, k)).iterator();
      return invokeNextAndHandle(ctx, command, (InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable ex) -> {
         WriteCommand writeCommand = (WriteCommand) rCommand;
         if (ex != null) {
            if (mustSyncInvalidation(invalidationFuture, writeCommand)) {
               return asyncValue(invalidationFuture).thenApply(rCtx, rCommand, (rCtx1, rCommand1, rv1) -> {
                  throw ex;
               });
            }
            throw ex;
         } else {
            if (mustSyncInvalidation(invalidationFuture, writeCommand)) {
               return asyncValue(invalidationFuture).thenApply(null, null, (rCtx2, rCommand2, rv2) -> MultiSubCommandInvoker.invokeEach(rCtx, subCommands, this, rv));
            } else {
               return MultiSubCommandInvoker.invokeEach(rCtx, subCommands, this, rv);
            }
         }
      });
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command)
         throws Throwable {
      for (Object key : invalidateL1Command.getKeys()) {
         abortL1UpdateOrWait(key);
         // If our invalidation was sent when the value wasn't yet cached but is still being requested the context
         // may not have the value - if so we need to add it then now that we know we waited for the get response
         // to complete
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapEntryForWriting(ctx, key, true, false);
         }
      }
      return invokeNext(ctx, invalidateL1Command);
   }

   private void abortL1UpdateOrWait(Object key) {
      L1WriteSynchronizer sync = concurrentWrites.remove(key);
      if (sync != null) {
         if (sync.trySkipL1Update()) {
            if (trace) {
               log.tracef("Aborted possible L1 update due to concurrent invalidation for key %s", key);
            }
         } else {
            if (trace) {
               log.tracef("L1 invalidation found a pending update for key %s - need to block until finished", key);
            }
            // We have to wait for the pending L1 update to complete before we can properly invalidate.  Any additional
            // gets that come in after this invalidation we ignore for now.
            boolean success;
            try {
               sync.get();
               success = true;
            } catch (InterruptedException e) {
               success = false;
               // Save the interruption status, but don't throw an explicit exception
               Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
               // We don't care what the L1 update exception was
               success = false;
            }
            if (trace) {
               log.tracef("Pending L1 update completed successfully: %b - L1 invalidation can occur for key %s", success, key);
            }
         }
      }
   }

   private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command,
         boolean assumeOriginKeptEntryInL1) {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         if (trace) {
            log.tracef("local mode forced, suppressing L1 calls.");
         }
         return invokeNext(ctx, command);
      }
      CompletableFuture<?> l1InvalidationFuture = invalidateL1InCluster(ctx, command, assumeOriginKeptEntryInL1);
      return invokeNextAndHandle(ctx, command, (InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable ex) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (ex != null) {
            if (mustSyncInvalidation(l1InvalidationFuture, dataWriteCommand)) {
               return asyncValue(l1InvalidationFuture).thenApply(rCtx, rCommand, (rCtx1, rCommand1, rv1) -> {
                  throw ex;
               });
            }
            throw ex;
         } else {
            if (mustSyncInvalidation(l1InvalidationFuture, dataWriteCommand)) {
               if (shouldRemoveFromLocalL1(rCtx, dataWriteCommand)) {
                  VisitableCommand removeFromL1Command = removeFromL1Command(rCtx, dataWriteCommand.getKey());
                  return makeStage(asyncInvokeNext(rCtx, removeFromL1Command, l1InvalidationFuture))
                        .thenApply(null, null, (rCtx2, rCommand2, rv2) -> rv);
               } else {
                  return asyncValue(l1InvalidationFuture).thenApply(rCtx, rCommand, (rCtx1, rCommand1, rv1) -> rv);
               }
            } else if (shouldRemoveFromLocalL1(rCtx, dataWriteCommand)) {
               VisitableCommand removeFromL1Command = removeFromL1Command(rCtx, dataWriteCommand.getKey());
               return invokeNextThenApply(rCtx, removeFromL1Command, (rCtx2, rCommand2, rv2) -> rv);
            } else if (trace) {
               log.trace("Allowing entry to commit as local node is owner");
            }
         }
         return rv;
      });
   }

   private boolean mustSyncInvalidation(CompletableFuture<?> invalidationFuture, WriteCommand writeCommand) {
      return invalidationFuture != null && !invalidationFuture.isDone() && isSynchronous(writeCommand);
   }

   private boolean shouldRemoveFromLocalL1(InvocationContext ctx, DataWriteCommand command) {
      return ctx.isOriginLocal() && !cdl.getCacheTopology().isWriteOwner(command.getKey());
   }

   private VisitableCommand removeFromL1Command(InvocationContext ctx, Object key) {
      if (trace) {
         log.tracef("Removing entry from L1 for key %s", key);
      }
      abortL1UpdateOrWait(key);
      ctx.removeLookedUpEntry(key);
      entryFactory.wrapEntryForWriting(ctx, key, true, false);

      return commandsFactory.buildInvalidateFromL1Command(EnumUtil.EMPTY_BIT_SET,
            Collections.singleton(key));
   }

   private CompletableFuture<?> invalidateL1InCluster(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) {
      CompletableFuture<?> l1InvalidationFuture = null;
      if (cdl.getCacheTopology().isWriteOwner(command.getKey())) {
         l1InvalidationFuture = l1Manager.flushCache(Collections.singletonList(command.getKey()), ctx.getOrigin(), assumeOriginKeptEntryInL1);
      } else if (trace) {
         log.tracef("Not invalidating key '%s' as local node(%s) is not owner", command.getKey(), rpcManager.getAddress());
      }
      return l1InvalidationFuture;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
