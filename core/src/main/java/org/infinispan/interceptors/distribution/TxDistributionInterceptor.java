package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
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
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
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

   @Inject
   public void inject(PartitionHandlingManager partitionHandlingManager) {
      this.partitionHandlingManager = partitionHandlingManager;
   }

   @Start
   public void start() {
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      useClusteredWriteSkewCheck = Configurations.isVersioningEnabled(cacheConfiguration);
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

      Object returnValue = handleTxWriteCommand(ctx, command, command.getKey(), command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER));
      if (ctx.isOriginLocal()) {
         // If the state transfer interceptor has to retry the command, it should ignore the previous value.
         command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
      }
      return returnValue;
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

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         //In Pessimistic mode, the delta composite keys were sent to the wrong owner and never locked.
         final Collection<Address> affectedNodes = cdl.getOwners(filterDeltaCompositeKeys(command.getKeys()));
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(affectedNodes == null ? dm.getConsistentHash()
               .getMembers() : affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(affectedNodes, command, rpcOptions);
         checkTxCommandResponses(responseMap, command, (LocalTxInvocationContext) ctx, ((LocalTxInvocationContext) ctx).getRemoteLocksAcquired());
      }
      return invokeNextInterceptor(ctx, command);
   }

   // ---- TX boundary commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, createCommitRpcOptions());
         checkTxCommandResponses(responseMap, command, (LocalTxInvocationContext) ctx, recipients);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = cdl.getOwners(getAffectedKeysFromContext(ctx));
         prepareOnAffectedNodes(ctx, command, recipients);
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients == null ? dm.getWriteConsistentHash()
               .getMembers() : recipients);
      }
      return retVal;
   }

   protected void prepareOnAffectedNodes(TxInvocationContext<?> ctx, PrepareCommand command, Collection<Address> recipients) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, createPrepareRpcOptions());
         checkTxCommandResponses(responseMap, command, (LocalTxInvocationContext) ctx, recipients);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = getCommitNodes(ctx);
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(recipients, command, createRollbackRpcOptions());
         checkTxCommandResponses(responseMap, command, (LocalTxInvocationContext) ctx, recipients);
      }

      return invokeNextInterceptor(ctx, command);
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      Collection<Address> affectedNodes = cdl.getOwners(getAffectedKeysFromContext(ctx));
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   protected void checkTxCommandResponses(Map<Address, Response> responseMap, TransactionBoundaryCommand command,
                                          LocalTxInvocationContext context, Collection<Address> recipients) {
      OutdatedTopologyException outdatedTopologyException = null;
      for (Map.Entry<Address, Response> e : responseMap.entrySet()) {
         Address recipient = e.getKey();
         Response response = e.getValue();
         if (response == CacheNotFoundResponse.INSTANCE) {
            // No need to retry if the missing node wasn't a member when the command started.
            if (command.getTopologyId() == stateTransferManager.getCacheTopology().getTopologyId()
                  && !rpcManager.getMembers().contains(recipient)) {
               if (trace) log.tracef("Ignoring response from node not targeted %s", recipient);
            } else {
               if (checkCacheNotFoundResponseInPartitionHandling(command, context, recipients)) {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing. It will be handled by the PartitionHandlingManager", recipient);
                  return;
               } else {
                  if (trace) log.tracef("Cache not running on node %s, or the node is missing", recipient);
                  //noinspection ThrowableInstanceNeverThrown
                  outdatedTopologyException = new OutdatedTopologyException(format("Cache not running on node %s, or the node is missing", recipient));
               }
            }
         } else if (response == UnsureResponse.INSTANCE) {
            if (trace) log.tracef("Node %s has a newer topology id", recipient);
            //noinspection ThrowableInstanceNeverThrown
            outdatedTopologyException = new OutdatedTopologyException(format("Node %s has a newer topology id", recipient));
         }
      }
      if (outdatedTopologyException != null) {
         throw outdatedTopologyException;
      }
   }

   private boolean checkCacheNotFoundResponseInPartitionHandling(TransactionBoundaryCommand command,
                                                                 LocalTxInvocationContext context,
                                                                 Collection<Address> recipients) {
      final GlobalTransaction globalTransaction = command.getGlobalTransaction();
      final Collection<Object> lockedKeys = context.getLockedKeys();
      if (command instanceof RollbackCommand) {
         return partitionHandlingManager.addPartialRollbackTransaction(globalTransaction, recipients, lockedKeys);
      } else if (command instanceof PrepareCommand) {
         if (((PrepareCommand) command).isOnePhaseCommit()) {
            return partitionHandlingManager.addPartialCommit1PCTransaction(globalTransaction, recipients, lockedKeys,
                                                                           Arrays.asList(((PrepareCommand) command).getModifications()));
         }
      } else if (command instanceof CommitCommand) {
         EntryVersionsMap newVersion = null;
         if (command instanceof VersionedCommitCommand) {
            newVersion = ((VersionedCommitCommand) command).getUpdatedVersions();
         }
         return partitionHandlingManager.addPartialCommit2PCTransaction(globalTransaction, recipients, lockedKeys, newVersion);
      }
      return false;
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

   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, Object key) throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      boolean skipRemoteGet =  entry != null && entry.skipLookup();
      if (skipRemoteGet) {
         return;
      }
      InternalCacheEntry ice = remoteGet(ctx, key, true, command);
      if (ice == null) {
         localGet(ctx, key, true, command, false);
      }
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

   private RpcOptions createCommitRpcOptions() {
      return createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncCommitPhase());
   }

   private RpcOptions createRollbackRpcOptions() {
      return createRpcOptionsFor2ndPhase(cacheConfiguration.transaction().syncRollbackPhase());
   }

   private RpcOptions createRpcOptionsFor2ndPhase(boolean sync) {
      if (sync) {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      } else {
         return rpcManager.getRpcOptionsBuilder(ResponseMode.ASYNCHRONOUS, DeliverOrder.NONE).build();
      }
   }

   protected RpcOptions createPrepareRpcOptions() {
      return cacheConfiguration.clustering().cacheMode().isSynchronous() ?
              rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build() :
              rpcManager.getDefaultRpcOptions(false);
   }
}
