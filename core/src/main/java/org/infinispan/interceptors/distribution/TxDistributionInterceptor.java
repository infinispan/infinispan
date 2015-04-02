package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKey;
import static org.infinispan.util.DeltaCompositeKeyUtil.filterDeltaCompositeKeys;
import static org.infinispan.util.DeltaCompositeKeyUtil.getAffectedKeysFromContext;

/**
 * Handles the distribution of the transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class TxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(TxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private PartitionHandlingManager partitionHandlingManager;

   private boolean isPessimisticCache;
   private boolean useClusteredWriteSkewCheck;
   private boolean partitionHandlingEnabled;

   private RpcOptions commitRpcOptions;
   private RpcOptions rollbackRpcOptions;

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Start
   public void start() {
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      useClusteredWriteSkewCheck = Configurations.isVersioningEnabled(cacheConfiguration);
      partitionHandlingEnabled = cacheConfiguration.clustering().partitionHandling().enabled();

      boolean partitionHandlingEnabled = cacheConfiguration.clustering().partitionHandling().enabled();
      rollbackRpcOptions = createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncRollbackPhase(),
                                                       partitionHandlingEnabled, rpcManager);

      commitRpcOptions = createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncCommitPhase(),
                                                     partitionHandlingEnabled, rpcManager);

   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return handleTxWriteCommand(ctx, command, command.getKey(), false);
      } finally {
         if (ctx.isOriginLocal()) {
            // If the state transfer interceptor has to retry the command, it should ignore the previous value.
            command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
         }
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return handleTxWriteCommand(ctx, command, command.getKey(), false);
      } finally {
         if (ctx.isOriginLocal()) {
            // If the state transfer interceptor has to retry the command, it should ignore the previous value.
            command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
         }
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         return handleNonTxWriteCommand(ctx, command);
      }

      try {
         return handleTxWriteCommand(ctx, command, command.getKey(), command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER));
      } finally {
         if (ctx.isOriginLocal()) {
            // If the state transfer interceptor has to retry the command, it should ignore the previous value.
            command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
         }
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return handleTxWriteCommand(ctx, command, null, true);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         return visitGetCommand(ctx, command, false);
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      try {
         return visitGetCommand(ctx, command, false);
      } catch (SuspectException e) {
         // retry
         return visitGetCacheEntryCommand(ctx, command);
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         //In Pessimistic mode, the delta composite keys were sent to the wrong owner and never locked.
         final Collection<Address> affectedNodes = cdl.getOwners(filterDeltaCompositeKeys(command.getKeys()));
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(affectedNodes == null ? dm.getConsistentHash().getMembers() : affectedNodes);
         if (trace) {
            log.tracef("Registered remote locks acquired %s", affectedNodes);
         }
         try {
            rpcManager.invokeRemotely(affectedNodes, command, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE));
         } catch (SuspectException e) {
            partitionHandlingManager.addPartialRollbackTransaction(command.getGlobalTransaction(), ((LocalTxInvocationContext) ctx).getRemoteLocksAcquired(), ctx.getLockedKeys(), e);
            //always throw exception
            throw e;
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         try {
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, commitRpcOptions);
            checkTxCommandResponses(responseMap);
         } catch (SuspectException e) {
            EntryVersionsMap newVersion = null;
            if (command instanceof VersionedCommitCommand) {
               newVersion = ((VersionedCommitCommand) command).getUpdatedVersions();
            }
            partitionHandlingManager.addPartialCommit2PCTransaction(command.getGlobalTransaction(), recipients,
                                                                         ctx.getLockedKeys(), newVersion, e);
         }

      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = cdl.getOwners(getAffectedKeysFromContext(ctx));

         try {
            prepareOnAffectedNodes(ctx, command, recipients, defaultSynchronous);
         } catch (SuspectException e) {
            if (command.isOnePhaseCommit()) {
               partitionHandlingManager.addPartialCommit1PCTransaction(command.getGlobalTransaction(), recipients,
                                                                       ctx.getLockedKeys(), Arrays.asList(command.getModifications()), e);
            } else {
               throw e;
            }
         }

         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients == null ? dm.getWriteConsistentHash().getMembers() : recipients);
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         try {
            Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, rollbackRpcOptions);
            checkTxCommandResponses(responseMap);
         } catch (SuspectException e) {
            partitionHandlingManager.addPartialRollbackTransaction(command.getGlobalTransaction(), recipients,
                                                                   ctx.getLockedKeys(), e);
         }
      }

      return invokeNextInterceptor(ctx, command);
   }

   protected void prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         RpcOptions rpcOptions;
         if (sync && command.isOnePhaseCommit() && !partitionHandlingEnabled) {
            rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
         } else {
            rpcOptions = rpcManager.getDefaultRpcOptions(sync);
         }
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, rpcOptions);
         checkTxCommandResponses(responseMap);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }

   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, Object key) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      boolean skipRemoteGet = entry != null && entry.skipLookup();
      if (skipRemoteGet) {
         return;
      }
      InternalCacheEntry ice = remoteGet(ctx, key, true, command);
      if (ice == null) {
         localGet(ctx, key, true, command, false);
      }
   }

   private Object visitGetCommand(InvocationContext ctx, AbstractDataCommand command,
                                  boolean isGetCacheEntry) throws Throwable {
      Object returnValue = invokeNextInterceptor(ctx, command);

      //if the cache entry has the value lock flag set, skip the remote get.
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      boolean skipRemoteGet = entry != null && entry.skipLookup();

      // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
      // available.  It could just have been removed in the same tx beforehand.  Also don't bother with a remote get if
      // the entry is mapped to the local node.
      if (!skipRemoteGet && returnValue == null && ctx.isOriginLocal()) {
         Object key = filterDeltaCompositeKey(command.getKey());
         if (needsRemoteGet(ctx, command)) {
            InternalCacheEntry ice = remoteGet(ctx, key, false, command);
            if (ice != null) {
               returnValue = ice.getValue();
            }
         }
         if (returnValue == null && !ctx.isEntryRemovedInContext(command.getKey())) {
            returnValue = localGet(ctx, key, false, command, isGetCacheEntry);
         }
      }
      return returnValue;
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      Collection<Address> affectedNodes = cdl.getOwners(getAffectedKeysFromContext(ctx));
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   protected void checkTxCommandResponses(Map<Address, Response> responseMap) {
      for (Map.Entry<Address, Response> e : responseMap.entrySet()) {
         Address recipient = e.getKey();
         Response response = e.getValue();
         // TODO Use a set to speed up the check?
         if (response instanceof CacheNotFoundResponse) {
            if (!rpcManager.getMembers().contains(recipient)) {
               log.tracef("Ignoring response from node not targeted %s", recipient);
            } else {
               log.tracef("Cache not running on node %s, or the node is missing", recipient);
               throw new OutdatedTopologyException("Cache not running on node " + recipient);
            }
         } else if (response instanceof UnsureResponse) {
            log.tracef("Node %s has a newer topology id", recipient);
            throw new OutdatedTopologyException("Cache not running on node " + recipient);
         }
      }
   }

   private boolean shouldFetchRemoteValuesForWriteSkewCheck(InvocationContext ctx, WriteCommand cmd) {
      // Note: the primary owner always already has the data, so this method is always going to return false
      if (useClusteredWriteSkewCheck && ctx.isInTxScope() && dm.isRehashInProgress()) {
         for (Object key : cmd.getAffectedKeys()) {
            boolean shouldPerformWriteSkewCheck = cdl.localNodeIsPrimaryOwner(key);
            // TODO Dan: remoteGet() already checks if the key is available locally or not
            if (shouldPerformWriteSkewCheck && dm.isAffectedByRehash(key) && !dataContainer.containsKey(key))
               return true;
         }
      }
      return false;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleTxWriteCommand(InvocationContext ctx, WriteCommand command, Object key, boolean skipRemoteGet) throws Throwable {
      // see if we need to load values from remote sources first
      if (!skipRemoteGet && needValuesFromPreviousOwners(ctx, command))
         remoteGetBeforeWrite(ctx, command, key);

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   protected boolean needValuesFromPreviousOwners(InvocationContext ctx, WriteCommand command) {
      if (ctx.isOriginLocal()) {
         // The return value only matters on the originator.
         // Conditional commands also check the previous value only on the originator.
         if (isNeedReliableReturnValues(command) || command.isConditional())
            return true;
      }
      return !command.hasFlag(Flag.CACHE_MODE_LOCAL) && (shouldFetchRemoteValuesForWriteSkewCheck(ctx, command) || command.hasFlag(Flag.DELTA_WRITE));
   }

   private Object localGet(InvocationContext ctx, Object key, boolean isWrite,
                           FlagAffectedCommand command, boolean isGetCacheEntry) throws Throwable {
      InternalCacheEntry ice = fetchValueLocallyIfAvailable(dm.getReadConsistentHash(), key);
      if (ice != null) {
         if (isWrite && isPessimisticCache && ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }
         if (!ctx.replaceValue(key, ice)) {
            if (isWrite)
               entryFactory.wrapEntryForPut(ctx, key, ice, false, command, false);
            else
               ctx.putLookedUpEntry(key, ice);
         }
         return isGetCacheEntry ? ice : ice.getValue();
      }
      return null;
   }

   private InternalCacheEntry remoteGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isValueAvailableLocally(dm.getReadConsistentHash(), key) || dm.isAffectedByRehash(key) && !dataContainer.containsKey(key)) {
         if (trace) log.tracef("Doing a remote get for key %s", key);

         boolean acquireRemoteLock = false;
         if (ctx.isInTxScope() && ctx.isOriginLocal()) {
            TxInvocationContext txContext = (TxInvocationContext) ctx;
            acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
         }
         // attempt a remote lookup
         InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command, isWrite);

         if (acquireRemoteLock) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }

         if (ice != null) {
            if (useClusteredWriteSkewCheck && ctx.isInTxScope()) {
               ((TxInvocationContext) ctx).getCacheTransaction().putLookedUpRemoteVersion(key, ice.getMetadata().version());
            }

            if (!ctx.replaceValue(key, ice)) {
               if (isWrite)
                  entryFactory.wrapEntryForPut(ctx, key, ice, false, command, false);
               else {
                  ctx.putLookedUpEntry(key, ice);
                  if (ctx.isInTxScope()) {
                     ((TxInvocationContext) ctx).getCacheTransaction().replaceVersionRead(key, ice.getMetadata().version());
                  }
               }
            }
            return ice;
         }
      } else {
         if (trace) {
            log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1. Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
         }
      }
      return null;
   }

   private static RpcOptions createRpcOptionsFor2ndPhase(boolean sync, boolean partitionHandlingEnabled,
                                                         RpcManager rpcManager) {
      if (partitionHandlingEnabled && sync) {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS, DeliverOrder.NONE).build();
      } else if (sync) {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      } else {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.NONE).build();
      }
   }
}
