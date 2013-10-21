package org.infinispan.interceptors.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Interceptor that handles L1 logic for non-transactional caches.
 *
 * @author Mircea Markus
 * @author William Burns
 * @since 5.2
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
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return performCommandWithL1WriteIfAble(ctx, command, false, true);
   }

   protected Object performCommandWithL1WriteIfAble(InvocationContext ctx, DataCommand command,
                                                boolean shouldAlwaysRunNextInterceptor, boolean registerL1) throws Throwable {
      Object returnValue;
      if (ctx.isOriginLocal()) {
         Object key = command.getKey();
         // If the command isn't going to return a remote value - just pass it down the interceptor chain
         if (skipL1Lookup(command, key)) {
            returnValue = invokeNextInterceptor(ctx, command);
         } else {
            returnValue = performL1Lookup(ctx, shouldAlwaysRunNextInterceptor, key, command);
         }
      } else {
         returnValue = invokeNextInterceptor(ctx, command);
         // If this is a remote command, and we found a value in our cache
         // we store it so that we can later invalidate it
         if (registerL1) {
            l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
         }
      }
      return returnValue;
   }

    protected Object performL1Lookup(InvocationContext ctx, boolean runInterceptorOnConflict, Object key,
                                     DataCommand command) throws Throwable {
      // Most times the putIfAbsent will be successful, so not doing a get first
      L1WriteSynchronizer l1WriteSync = new L1WriteSynchronizer(dataContainer, l1Lifespan, stateTransferLock,
                                                                cdl);
      L1WriteSynchronizer presentSync = concurrentWrites.putIfAbsent(key, l1WriteSync);
      if (presentSync == null) {
         try {
            l1Manager.registerL1WriteSynchronizer(key, l1WriteSync);
            Object returnValue;
            try {
               returnValue = invokeNextInterceptor(ctx, command);
            }
            finally {
               l1Manager.unregisterL1WriteSynchronizer(key);
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
            Object returnValue = presentSync.get();
            // Basically write commands could have different values
            if (runInterceptorOnConflict) {
               returnValue = invokeNextInterceptor(ctx, command);
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
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command, true);
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
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Future<Object> invalidationFuture = null;
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

      Object result = invokeNextInterceptor(ctx, command);
      processInvalidationResult(ctx, command, invalidationFuture);
      //we also need to remove from L1 the keys that are not ours
      for (Object o : command.getAffectedKeys()) {
         if (!cdl.localNodeIsOwner(o)) {
            removeFromL1(ctx, o);
         }
      }
      return result;
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
      for (Object key : invalidateL1Command.getKeys()) {
         abortL1UpdateOrWait(key);
      }
      return super.visitInvalidateL1Command(ctx, invalidateL1Command);
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
            // TODO: Additional gets should be fixed with ISPN-2965
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

   private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) throws Throwable {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) {
            log.tracef("local mode forced, suppressing L1 calls.");
         }
         return invokeNextInterceptor(ctx, command);
      }
      Future<Object> l1InvalidationFuture = invalidateL1(ctx, command, assumeOriginKeptEntryInL1);
      Object returnValue = invokeNextInterceptor(ctx, command);
      processInvalidationResult(ctx, command, l1InvalidationFuture);
      removeFromLocalL1(ctx, command);
      return returnValue;
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
      entryFactory.wrapEntryForRemove(ctx, key, true);

      InvalidateCommand command = commandsFactory.buildInvalidateFromL1Command(false, null, Collections.singleton(key));
      invokeNextInterceptor(ctx, command);
   }

   private void processInvalidationResult(InvocationContext ctx, FlagAffectedCommand command, Future<Object> l1InvalidationFuture) throws InterruptedException, ExecutionException {
      if (l1InvalidationFuture != null) {
         if (isSynchronous(command)) {
            l1InvalidationFuture.get();
         }
      }
   }

   private Future<Object> invalidateL1(InvocationContext ctx, DataWriteCommand command, boolean assumeOriginKeptEntryInL1) {
      Future<Object> l1InvalidationFuture = null;
      if (cdl.localNodeIsOwner(command.getKey())) {
         l1InvalidationFuture = l1Manager.flushCache(Collections.singletonList(command.getKey()), ctx.getOrigin(), assumeOriginKeptEntryInL1);
      } else if (trace) {
         log.tracef("Not invalidating key '%' as local node(%s) is not owner", command.getKey(), rpcManager.getAddress());
      }
      return l1InvalidationFuture;
   }
}
