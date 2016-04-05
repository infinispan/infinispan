package org.infinispan.interceptors.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Interceptor that handles L1 logic for non-transactional caches.
 *
 * @author Mircea Markus
 * @author William Burns
 */
public class L1NonTxInterceptor extends BaseRpcInterceptor {

   private static final Log log = LogFactory.getLog(L1NonTxInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   protected L1Manager l1Manager;
   protected ClusteringDependentLogic cdl;
   protected EntryFactory entryFactory;
   protected CommandsFactory commandsFactory;
   protected DataContainer dataContainer;
   protected Configuration config;
   protected StateTransferLock stateTransferLock;

   private long l1Lifespan;
   private long replicationTimeout;

   /**
    *  This map holds all the current write synchronizers registered for a given key.  This map is only added to when an
    * operation is invoked that would cause a remote get to occur (which is controlled by whether or not the
    * {@link L1NonTxInterceptor#skipL1Lookup(LocalFlagAffectedCommand, Object)} method returns
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

   @Inject
   public void init(L1Manager l1Manager, ClusteringDependentLogic cdl, EntryFactory entryFactory,
                    DataContainer dataContainer, Configuration config, StateTransferLock stateTransferLock,
                    CommandsFactory commandsFactory) {
      this.l1Manager = l1Manager;
      this.cdl = cdl;
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
      this.config = config;
      this.stateTransferLock = stateTransferLock;
      this.commandsFactory = commandsFactory;
   }

   @Start
   public void start() {
      l1Lifespan = config.clustering().l1().lifespan();
      replicationTimeout = config.clustering().remoteTimeout();
   }

   @Override
   public final CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command, false);
   }
   @Override
   public final CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return visitDataReadCommand(ctx, command, true);
   }
   private CompletableFuture<Void> visitDataReadCommand(InvocationContext ctx, AbstractDataCommand command, boolean isEntry) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, isEntry, false, true);
   }

   protected CompletableFuture<Void> performCommandWithL1WriteIfAble(InvocationContext ctx, DataCommand command, boolean isEntry,
                                                boolean shouldAlwaysRunNextInterceptor, boolean registerL1) throws Throwable {
      Object returnValue;
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         // If the command isn't going to return a remote value - just pass it down the interceptor chain
         if (skipL1Lookup(command, key)) {
            returnValue = ctx.forkInvocationSync(command);
         } else {
            returnValue = performL1Lookup(ctx, shouldAlwaysRunNextInterceptor, key, command, isEntry);
         }
      } else {
         // If this is a remote command, and we found a value in our cache
         // we store it so that we can later invalidate it
         if (registerL1) {
            l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
         }
         returnValue = ctx.forkInvocationSync(command);
      }
      return ctx.shortCircuit(returnValue);
   }

   protected Object performL1Lookup(InvocationContext ctx, boolean runInterceptorOnConflict, Object key,
                                     DataCommand command, boolean isEntry) throws Throwable {
      // Most times the putIfAbsent will be successful, so not doing a get first
      L1WriteSynchronizer l1WriteSync = new L1WriteSynchronizer(dataContainer, l1Lifespan, stateTransferLock,
                                                                cdl);
      L1WriteSynchronizer presentSync = concurrentWrites.putIfAbsent(key, l1WriteSync);

      // If the sync was null that means we are the first to register for the given key.  If not that means there is
      // a concurrent request that also wants to do a remote get for the key.  If there was another thread requesting
      // the key we should wait until they get the value instead of doing another remote get.
      if (presentSync == null) {
         try {
            // Note this is the same synchronizer we just created that is registered with the L1Manager
            l1Manager.registerL1WriteSynchronizer(key, l1WriteSync);
            Object returnValue;
            try {
               returnValue = ctx.forkInvocationSync(command);
            }
            finally {
               l1Manager.unregisterL1WriteSynchronizer(key, l1WriteSync);
            }
            return returnValue;
         }
         catch (Throwable t) {
            l1WriteSync.retrievalEncounteredException(t);
            throw t;
         }
         finally {
            concurrentWrites.remove(key);
         }
      } else {
         if (trace) {
            log.tracef("Found current request for key %s, waiting for their invocation's response", key);
         }
         try {
            Object returnValue;
            try {
               returnValue = presentSync.get(replicationTimeout, TimeUnit.MILLISECONDS);
               // Write commands could have different values so we always want to run them after we know the remote
               // value is retrieved.  Gets however only need the return value so we don't need to run the additional
               // interceptors
               if (runInterceptorOnConflict) {
                  returnValue = ctx.forkInvocationSync(command);
               } else if (!isEntry && returnValue instanceof InternalCacheEntry) {
                  returnValue = ((InternalCacheEntry)returnValue).getValue();
               }
            } catch (TimeoutException e) {
               // This should never be required since the status is always set in a try catch above - but IBM doesn't...
               log.warnf("Synchronizer didn't return in %s milliseconds - running command normally!", replicationTimeout);
               // Always run next interceptor if a timeout occurs
               returnValue = ctx.forkInvocationSync(command);
            }
            return returnValue;
         }
         catch (ExecutionException e) {
            throw e.getCause();
         }
      }
   }

   protected boolean skipL1Lookup(LocalFlagAffectedCommand command, Object key) {
      return command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            || command.hasFlag(Flag.IGNORE_RETURN_VALUES) || cdl.localNodeIsOwner(key)
            || dataContainer.containsKey(key);
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, false);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Future<?> invalidationFuture = null;
      Set<Object> keys = command.getMap().keySet();
      Set<Object> toInvalidate = new HashSet<Object>(keys.size());
      for (Object k : keys) {
         if (cdl.localNodeIsOwner(k)) {
            toInvalidate.add(k);
         }
      }
      if (!toInvalidate.isEmpty()) {
         invalidationFuture = l1Manager.flushCache(toInvalidate, ctx.getOrigin(), true);
      }

      Object result = ctx.forkInvocationSync(command);
      processInvalidationResult(command, invalidationFuture);
      //we also need to remove from L1 the keys that are not ours
      for (Object o : command.getAffectedKeys()) {
         if (!cdl.localNodeIsOwner(o)) {
            removeFromL1(ctx, o);
         }
      }
      return ctx.shortCircuit(result);
   }

   @Override
   public CompletableFuture<Void> visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
      for (Object key : invalidateL1Command.getKeys()) {
         abortL1UpdateOrWait(key);
         // If our invalidation was sent when the value wasn't yet cached but is still being requested the context
         // may not have the value - if so we need to add it then now that we know we waited for the get response
         // to complete
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, true, true);
         }
      }
      return ctx.continueInvocation();
   }

   private void abortL1UpdateOrWait(Object key) throws InterruptedException {
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

   private CompletableFuture<Void> handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) throws Throwable {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) {
            log.tracef("local mode forced, suppressing L1 calls.");
         }
         return ctx.continueInvocation();
      }
      Future<?> l1InvalidationFuture = invalidateL1(ctx, command, assumeOriginKeptEntryInL1);
      Object returnValue = ctx.forkInvocationSync(command);
      processInvalidationResult(command, l1InvalidationFuture);
      removeFromLocalL1(ctx, command);
      return ctx.shortCircuit(returnValue);
   }

   private void removeFromLocalL1(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !cdl.localNodeIsOwner(command.getKey())) {
         removeFromL1(ctx, command.getKey());
      } else if (trace) {
         log.trace("Allowing entry to commit as local node is owner");
      }
   }

   private void removeFromL1(InvocationContext ctx, Object key) throws Throwable {
      if (trace) {
         log.tracef("Removing entry from L1 for key %s", key);
      }
      abortL1UpdateOrWait(key);
      ctx.removeLookedUpEntry(key);
      entryFactory.wrapEntryForWriting(ctx, key, EntryFactory.Wrap.WRAP_NON_NULL, true, true);

      InvalidateCommand command = commandsFactory.buildInvalidateFromL1Command(EnumUtil.EMPTY_BIT_SET, Collections.singleton(key));
      ctx.forkInvocationSync(command);
   }

   private void processInvalidationResult(FlagAffectedCommand command, Future<?> l1InvalidationFuture) throws InterruptedException, ExecutionException {
      if (l1InvalidationFuture != null) {
         if (isSynchronous(command)) {
            l1InvalidationFuture.get();
         }
      }
   }

   private Future<?> invalidateL1(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) {
      Future<?> l1InvalidationFuture = null;
      if (cdl.localNodeIsOwner(command.getKey())) {
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
