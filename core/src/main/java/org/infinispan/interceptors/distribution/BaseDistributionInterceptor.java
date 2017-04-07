package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.StrictOrderingCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Immutables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for distribution of entries across a cluster.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dan@infinispan.org>
 */
public abstract class BaseDistributionInterceptor extends ClusteringInterceptor {
   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final long REPLICATE_NON_AUTHORITATIVE_FLAGS =
         FlagBitSets.COMMAND_RETRY | FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.PROVIDED_RESULT;

   protected DistributionManager dm;
   protected RemoteValueRetrievedListener rvrl;
   protected KeyPartitioner keyPartitioner;
   protected ClusteringDependentLogic cdl;

   protected boolean isL1Enabled;
   protected boolean isReplicated;

   private final ReadOnlyManyHelper readOnlyManyHelper = new ReadOnlyManyHelper();
   private final InvocationSuccessFunction primaryReturnHandler = this::primaryReturnHandler;
   protected TimeService timeService;

   protected final PutMapHelper putMapHelper = new PutMapHelper();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager, RemoteValueRetrievedListener rvrl,
                                  KeyPartitioner keyPartitioner, TimeService timeService, ClusteringDependentLogic cdl) {
      this.dm = distributionManager;
      this.rvrl = rvrl;
      this.keyPartitioner = keyPartitioner;
      this.timeService = timeService;
      this.cdl = cdl;
   }


   @Start
   public void configure() {
      // Can't rely on the super injectConfiguration() to be called before our injectDependencies() method2
      isL1Enabled = cacheConfiguration.clustering().l1().enabled();
      isReplicated = cacheConfiguration.clustering().cacheMode().isReplicated();
   }

   @Override
   public final Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      final String groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      Address primaryOwner = dm.getCacheTopology().getDistribution(groupName).primary();
      CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(
            Collections.singleton(primaryOwner), command, defaultSyncOptions);
      return asyncInvokeNext(ctx, command, future.thenAccept(responses -> {
         if (!responses.isEmpty()) {
            Response response = responses.values().iterator().next();
            if (response instanceof SuccessfulResponse) {
               //noinspection unchecked
               List<CacheEntry> cacheEntries = (List<CacheEntry>) ((SuccessfulResponse) response).getResponseValue();
               for (CacheEntry entry : cacheEntries) {
                  wrapRemoteEntry(ctx, entry.getKey(), entry, false);
               }
            }
         }
      }));
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(
               isSynchronous(command) ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS).build();
         return asyncInvokeNext(ctx, command, rpcManager.invokeRemotelyAsync(null, command, rpcOptions));
      }
      return invokeNext(ctx, command);
   }

   protected CompletableFuture<Void> remoteGet(InvocationContext ctx, Object key, boolean isWrite,
                                               int topologyId, long flagsBitSet) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(topologyId);
      int currentTopologyId = cacheTopology.getTopologyId();

      DistributionInfo info = cacheTopology.getDistribution(key);
      if (info.isReadOwner()) {
         if (trace) {
            log.tracef("Key %s became local after wrapping, retrying command. Command topology is %d, current topology is %d",
                  key, topologyId, currentTopologyId);
         }
         // The topology has changed between EWI and BDI, let's retry
         if (topologyId == currentTopologyId) {
            throw new IllegalStateException();
         }
         throw new OutdatedTopologyException(currentTopologyId);
      }
      if (trace) {
         log.tracef("Perform remote get for key %s. currentTopologyId=%s, owners=%s",
            key, currentTopologyId, info.readOwners());
      }

      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, flagsBitSet);
      getCommand.setTopologyId(currentTopologyId);
      getCommand.setWrite(isWrite);

      return rpcManager.invokeRemotelyAsync(info.readOwners(), getCommand, getStaggeredOptions(info.readOwners().size())).thenAccept(responses -> {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               SuccessfulResponse response = (SuccessfulResponse) r;
               Object responseValue = response.getResponseValue();
               if (responseValue == null) {
                  if (rvrl != null) {
                     rvrl.remoteValueNotFound(key);
                  }
                  wrapRemoteEntry(ctx, key, NullCacheEntry.getInstance(), isWrite);
                  return;
               }
               InternalCacheEntry ice = ((InternalCacheValue) responseValue).toInternalCacheEntry(key);
               if (rvrl != null) {
                  rvrl.remoteValueFound(ice);
               }
               wrapRemoteEntry(ctx, key, ice, isWrite);
               return;
            }
         }
         throw handleMissingSuccessfulResponse(responses);
      });
   }

   protected static CacheException handleMissingSuccessfulResponse(Map<Address, Response> responses) {
      // The response map does not contain any ExceptionResponses; these are rethrown as exceptions
      if (responses.values().stream().anyMatch(UnsureResponse.class::isInstance)) {
         // We got only unsure responses, as all nodes that were read-owners at the time when we've sent
         // the request have progressed to newer topology. However we are guaranteed to have progressed
         // to a topology at most one older, and can immediately retry.
         return OutdatedTopologyException.INSTANCE;
      } else {
         // Another instance when we don't get any successful response is when all owners are lost. We'll handle
         // this later in StateTransferInterceptor, as we have to signal this to PartitionHandlingInterceptor
         // if that's present.
         return AllOwnersLostException.INSTANCE;
      }
   }

   protected void wrapRemoteEntry(InvocationContext ctx, Object key, CacheEntry ice, boolean isWrite) {
      entryFactory.wrapExternalEntry(ctx, key, ice, true, isWrite);
   }

   protected final Object handleNonTxWriteCommand(InvocationContext ctx, AbstractDataWriteCommand command)
         throws Throwable {
      Object key = command.getKey();

      if (isLocalModeForced(command)) {
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
         command.setAuthoritative(true);
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      DistributionInfo distributionInfo = cacheTopology.getDistribution(key);

      if (distributionInfo.isPrimary()) {
         if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
            ByRef<Object> returnValue = new ByRef<>(null);
            if (checkInvocationRecord(ctx, command, distributionInfo, returnValue, this::updateBackupsAndReturn)) {
               return returnValue.get();
            }
         }
         if (command instanceof StrictOrderingCommand) {
            CommandInvocationId lastId = ctx.lookupEntry(key)
                  .metadata().flatMap(Metadata::lastInvocationOpt).map(InvocationRecord::getId).orElse(null);
            if (lastId != null) {
               ((StrictOrderingCommand) command).setLastInvocationId(command.getKey(), lastId);
            }
         }

         return invokeNextThenApply(ctx, command, primaryReturnHandler);
      } else if (ctx.isOriginLocal()) {
         return invokeRemotely(command, distributionInfo);
      } else {
         return handleBackupWrite(ctx, command, distributionInfo);
      }
   }

   protected Object handleBackupWrite(InvocationContext ctx, AbstractDataWriteCommand command, DistributionInfo distributionInfo) {
      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (command instanceof StrictOrderingCommand) {
         StrictOrderingCommand soc = (StrictOrderingCommand) command;
         if (soc.isStrictOrdering()) {
            CommandInvocationId lastInvocationId = soc.getLastInvocationId(command.getKey());
            CommandInvocationId entryInvocationId = Optional.ofNullable(entry)
                  .flatMap(CacheEntry::metadata).flatMap(Metadata::lastInvocationOpt)
                  .map(InvocationRecord::getId).orElse(null);
            if (entry != null && Objects.equals(lastInvocationId, entryInvocationId)) {
               // We can apply the write, because it's based on the same previous value
               // Note: previous invocation id might expire on primary, therefore if it sends a command without
               // valid last invocation id and we are not read owner, we cannot apply the value because primary
               // *could* have a value (without any id)
            } else {
               return retrieveRemoteAndMaybeApply(ctx, command, distributionInfo, lastInvocationId, command.getKey());
            }
         }
      }
      if (entry == null) {
         // Retrieving remote value may be impossible as it might be already overwritten on all owners.
         assert command.loadType() != VisitableCommand.LoadType.OWNER;
         entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
      } else if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         Metadata metadata = entry.getMetadata();
         if (metadata != null) {
            InvocationRecord invocationRecord = metadata.invocation(command.getCommandInvocationId());
            if (invocationRecord != null) {
               command.setCompleted(command.getKey());
               cdl.notifyCommitEntry(invocationRecord.isCreated(), invocationRecord.isRemoved(), false,
                     entry, ctx, command, null, null);
               return invocationRecord.returnValue;
            }
         }
      }
      command.setAuthoritative(false);
      return invokeNext(ctx, command);
   }

   private Object retrieveRemoteAndMaybeApply(InvocationContext ctx, AbstractDataWriteCommand command, DistributionInfo distributionInfo, CommandInvocationId lastInvocationId, Object key) {
      ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(key, FlagBitSets.WITH_INVOCATION_RECORDS);
      clusteredGetCommand.setTopologyId(command.getTopologyId());
      return asyncValue(rpcManager.invokeRemotelyAsync(distributionInfo.primaryAsList(), clusteredGetCommand, defaultSyncOptions)
            .thenApply(responses -> {
               ByRef<Object> retval = new ByRef<>(null);
               if (handleStrictOrderingResponse(responses, ctx, command, key, distributionInfo, lastInvocationId, retval::set)) {
                  return invokeNext(ctx, command);
               } else {
                  return retval.get();
               }
            }));
   }

   protected boolean handleStrictOrderingResponse(Map<Address, Response> responses, InvocationContext ctx, WriteCommand command, Object key, DistributionInfo distributionInfo, CommandInvocationId lastInvocationId, Consumer<Object> completedResultsConsumer) {
      ValidResponse rsp = getResponseFromPrimaryOwner(distributionInfo.primary(), responses);
      if (!(rsp instanceof SuccessfulResponse)) {
         throw OutdatedTopologyException.INSTANCE;
      }

      CacheEntry<?, ?> cacheEntry;
      CommandInvocationId id;
      if (rsp.getResponseValue() == null) {
         cacheEntry = NullCacheEntry.getInstance();
         id = null;
      } else {
         cacheEntry = ((InternalCacheValue) rsp.getResponseValue()).toInternalCacheEntry(key);
         id = cacheEntry.metadata().flatMap(Metadata::lastInvocationOpt).map(InvocationRecord::getId).orElse(null);
      }
      wrapRemoteEntry(ctx, key, cacheEntry, true);

      if (id == null || id.equals(lastInvocationId)) {
         // One of these cases:
         // 1) id == null -> the previous invocation id has expired, but as commands on backup are
         // invoked in order, this means that this is the correct previous value (as opposed to
         // a more recent one that already has the change applied
         // 2) We got the value before applying the command and should apply the write
         if (trace)
            log.tracef("Continuing with command execution, entry's last id is %s, requested last id is %s", id, lastInvocationId);
         command.setAuthoritative(false);
         return true;
      } else {
         // We got other value and we should commit it. This boils down to these cases:
         // 1) id == command.getCommandInvocationId() -> the command was already applied & committed on primary
         // 2) id == another id -> triangle-like algorithm, we've received more recent value,
         // we'll use it. This will result in further retrievals (and some events being lost),
         // but in correct value in the end.
         if (trace)
            log.trace("Command was already applied");
         CacheEntry entry = ctx.lookupEntry(key);
         entry.setChanged(true);
         InvocationRecord invocationRecord = cacheEntry.metadata()
               .flatMap(m -> Optional.ofNullable(m.invocation(command.getCommandInvocationId())))
               .orElseThrow(() -> new IllegalStateException("Retrieved " + cacheEntry + " for command "
                     + command + " but missing id " + command.getCommandInvocationId() + " in records"));
         // We're not marking the command as completed because we want to persist the modified value in context
         completedResultsConsumer.accept(invocationRecord.returnValue);
         return false;
      }
   }

   private Object invokeRemotely(DataWriteCommand command, DistributionInfo distributionInfo) {
      if (trace) log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", distributionInfo.primary());
      boolean isSyncForwarding = isSynchronous(command) || command.isReturnValueExpected();

      CompletableFuture<Map<Address, Response>> remoteInvocation = rpcManager.invokeRemotelyAsync(distributionInfo.primaryAsList(), command,
               isSyncForwarding ? defaultSyncOptions : defaultAsyncOptions);
      if (isSyncForwarding) {
         return asyncValue(remoteInvocation.handle((responses, t) -> {
            CompletableFutures.rethrowException(t);

            ValidResponse primaryResponse = getResponseFromPrimaryOwner(distributionInfo.primary(), responses);
            if (!primaryResponse.isSuccessful()) {
               command.fail();
            }
            // We expect only successful/unsuccessful responses, not unsure
            return primaryResponse.getResponseValue();
         }));
      } else {
         return null;
      }
   }

   private Object primaryReturnHandler(InvocationContext ctx, VisitableCommand visitableCommand, Object localResult) {
      DataWriteCommand command = (DataWriteCommand) visitableCommand;
      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      DistributionInfo distributionInfo = cacheTopology.getDistribution(command.getKey());
      if (command.isSuccessful()) {
         return updateBackupsAndReturn(command, distributionInfo, localResult);
      } else {
         return handleUnsuccessfulWriteOnPrimary(ctx, localResult, command.getKey(), distributionInfo);
      }
   }

   private Object updateBackupsAndReturn(DataWriteCommand command, DistributionInfo distributionInfo, Object localResult) {
      Collection<Address> backups = distributionInfo.writeBackups();
      if (backups.isEmpty()) {
         // There are no backups, skip the replication part.
         return localResult;
      }
      Collection<Address> recipients = isReplicated ? null : backups;

      RpcOptions rpcOptions = determineRpcOptionsForBackupReplication(rpcManager, isSynchronous(command), recipients);
      // TODO: set flags so that backup does not try to return any value
      // but local interceptors can consume it - SKIP_REMOTE_LOOKUP?
      CompletableFuture<Map<Address, Response>> remoteInvocation =
            rpcManager.invokeRemotelyAsync(recipients, command, rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         CompletableFutures.rethrowException(t instanceof RemoteException ? t.getCause() : t);
         return localResult;
      }));
   }

   private RpcOptions determineRpcOptionsForBackupReplication(RpcManager rpc, boolean isSync, Collection<Address> recipients) {
      if (isSync) {
         // If no recipients, means a broadcast, so we can ignore leavers
         return recipients == null ?
               rpc.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS).build() :
               defaultSyncOptions;
      } else {
         return defaultAsyncOptions;
      }
   }

   private ValidResponse getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
      Response fromPrimaryOwner = addressResponseMap.get(primaryOwner);
      if (fromPrimaryOwner == null) {
         throw new IllegalStateException("Missing response from primary owner!");
      }
      if (fromPrimaryOwner.isValid()) {
         return (ValidResponse) fromPrimaryOwner;
      }
      if (fromPrimaryOwner instanceof CacheNotFoundResponse) {
         // This means the cache wasn't running on the primary owner, so the command wasn't executed.
         // We throw an OutdatedTopologyException, StateTransferInterceptor will catch the exception and
         // it will then retry the command.
         throw new OutdatedTopologyException("Cache is no longer running on primary owner " + primaryOwner);
      }
      Throwable cause = fromPrimaryOwner instanceof ExceptionResponse ? ((ExceptionResponse)fromPrimaryOwner).getException() : null;
      throw new CacheException("Got unexpected response from primary owner: " + fromPrimaryOwner, cause);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         return invokeNext(ctx, command);
      }

      if (!ctx.isOriginLocal()) {
         for (Object key : command.getKeys()) {
            if (ctx.lookupEntry(key) == null) {
               return UnsureResponse.INSTANCE;
            }
         }
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, command.getKeys(), cacheTopology, null);
      if (requestedKeys.isEmpty()) {
         return invokeNext(ctx, command);
      }

      CompletableFuture<Void> allFuture = remoteGetAll(ctx, command, requestedKeys);
      return asyncInvokeNext(ctx, command, allFuture);
   }

   protected CompletableFuture<Void> remoteGetAll(InvocationContext ctx, GetAllCommand command, Map<Address, List<Object>> requestedKeys) {
      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      CompletableFutureWithCounter allFuture = new CompletableFutureWithCounter(requestedKeys.size());

      for (Map.Entry<Address, List<Object>> pair : requestedKeys.entrySet()) {
         List<Object> keys = pair.getValue();
         ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(keys, command.getFlagsBitSet(), gtx);
         rpcManager.invokeRemotelyAsync(Collections.singleton(pair.getKey()), clusteredGetAllCommand, defaultSyncOptions).whenComplete((responseMap, throwable) -> {
            if (throwable != null) {
               allFuture.completeExceptionally(throwable);
            }
            Response response = getSingleSuccessfulResponseOrFail(responseMap, allFuture);
            if (response == null) return;
            Object responseValue = ((SuccessfulResponse) response).getResponseValue();
            if (responseValue instanceof InternalCacheValue[]) {
               InternalCacheValue[] values = (InternalCacheValue[]) responseValue;
               int counterValue;
               synchronized (allFuture) {
                  for (int i = 0; i < keys.size(); ++i) {
                     Object key = keys.get(i);
                     InternalCacheValue value = values[i];
                     CacheEntry entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
                     wrapRemoteEntry(ctx, key, entry, false);
                  }
                  counterValue = --allFuture.counter;
               }
               // complete the future after sync block!
               if (counterValue == 0) {
                  allFuture.complete(null);
               }
            } else {
               allFuture.completeExceptionally(new IllegalStateException("Unexpected response value: " + responseValue));
            }
         });
      }
      return allFuture;
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleFunctionalReadManyCommand(ctx, command, readOnlyManyHelper);
   }

   protected <C extends TopologyAffectedCommand & FlagAffectedCommand> Object handleFunctionalReadManyCommand(
         InvocationContext ctx, C command, ReadManyCommandHelper<C> helper) {
      // We cannot merge this method with visitGetAllCommand because this can't wrap entries into context
      // TODO: repeatable-reads are not implemented - see visitReadOnlyKeyCommand
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         return handleLocalOnlyReadManyCommand(ctx, command, helper.keys(command));
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      Collection<?> keys = helper.keys(command);
      if (!ctx.isOriginLocal()) {
         return handleRemoteReadManyCommand(ctx, command, keys, helper);
      }
      if (keys.isEmpty()) {
         return Stream.empty();
      }

      ConsistentHash ch = cacheTopology.getReadConsistentHash();
      int estimateForOneNode = 2 * keys.size() / ch.getMembers().size();
      List<Object> availableKeys = new ArrayList<>(estimateForOneNode);
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, keys, cacheTopology, availableKeys);

      // TODO: while this works in a non-blocking way, the returned stream is not lazy as the functional
      // contract suggests. Traversable is also not honored as it is executed only locally on originator.
      // On FutureMode.ASYNC, there should be one command per target node going from the top level
      // to allow retries in StateTransferInterceptor in case of topology change.
      MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(
         ctx, requestedKeys.size() + (availableKeys.isEmpty() ? 0 : 1),
         new Object[keys.size()], helper::transformResult);

      handleLocallyAvailableKeys(ctx, command, availableKeys, allFuture, helper);
      int pos = availableKeys.size();
      for (Map.Entry<Address, List<Object>> addressKeys : requestedKeys.entrySet()) {
         List<Object> keysForAddress = addressKeys.getValue();
         remoteReadMany(addressKeys.getKey(), keysForAddress, ctx, command, allFuture, pos, helper);
         pos += keysForAddress.size();
      }
      return asyncValue(allFuture);
   }

   private Object handleLocalOnlyReadManyCommand(InvocationContext ctx, VisitableCommand command, Collection<?> keys) {
      for (Object key : keys) {
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), true, false);
         }
      }
      return invokeNext(ctx, command);
   }

   private <C extends TopologyAffectedCommand & VisitableCommand> Object handleRemoteReadManyCommand(
         InvocationContext ctx, C command, Collection<?> keys, InvocationSuccessFunction remoteReturnHandler) {
      for (Object key : keys) {
         if (ctx.lookupEntry(key) == null) {
            return UnsureResponse.INSTANCE;
         }
      }
      return invokeNextThenApply(ctx, command, remoteReturnHandler);
   }

   private <C extends ReplicableCommand> void remoteReadMany(Address owner, List<Object> keys,
                                                             InvocationContext ctx, C command,
                                                             MergingCompletableFuture<Object> allFuture,
                                                             int destinationIndex,
                                                             ReadManyCommandHelper<C> helper) {
      ReplicableCommand remoteCommand = helper.copyForRemote(command, keys, ctx);
      rpcManager.invokeRemotelyAsync(Collections.singleton(owner), remoteCommand, defaultSyncOptions)
         .whenComplete((responseMap, throwable) -> {
            if (throwable != null) {
               allFuture.completeExceptionally(throwable);
            }
            Response response = getSingleSuccessfulResponseOrFail(responseMap, allFuture);
            if (response == null) return;
            try {
               Object responseValue = ((SuccessfulResponse) response).getResponseValue();
               Object[] values = unwrapFunctionalManyResultOnOrigin(ctx, keys, responseValue);
               if (values != null) {
                  System.arraycopy(values, 0, allFuture.results, destinationIndex, values.length);
                  allFuture.countDown();
               } else {
                  allFuture.completeExceptionally(new IllegalStateException("Unexpected response value " + responseValue));
               }
            } catch (Throwable t) {
               allFuture.completeExceptionally(t);
            }
         });
   }

   private <C extends VisitableCommand> void handleLocallyAvailableKeys(
         InvocationContext ctx, C command, List<Object> availableKeys,
         MergingCompletableFuture<Object> allFuture, ReadManyCommandHelper<C> helper) {
      if (availableKeys.isEmpty()) {
         return;
      }
      C localCommand = helper.copyForLocal(command, availableKeys);
      invokeNextAndHandle(ctx, localCommand, (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else {
            try {
               helper.applyLocalResult(allFuture, rv);
               allFuture.countDown();
            } catch (Throwable t) {
               allFuture.completeExceptionally(t);
            }
         }
         return asyncValue(allFuture);
      });
   }

   private Map<Address, List<Object>> getKeysByOwner(InvocationContext ctx, Collection<?> keys,
                                                     LocalizedCacheTopology cacheTopology,
                                                     List<Object> availableKeys) {
      int capacity = cacheTopology.getMembers().size();
      Map<Address, List<Object>> requestedKeys = new HashMap<>(capacity);
      int estimateForOneNode = 2 * keys.size() / capacity;
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            DistributionInfo distributionInfo = cacheTopology.getDistribution(key);
            // Let's try to minimize the number of messages by preferring owner to which we've already
            // decided to send message
            boolean foundExisting = false;
            for (Address address : distributionInfo.readOwners()) {
               if (address.equals(rpcManager.getAddress())) {
                  throw new IllegalStateException("Entry should be always wrapped!");
               }
               List<Object> list = requestedKeys.get(address);
               if (list != null) {
                  list.add(key);
                  foundExisting = true;
                  break;
               }
            }
            if (!foundExisting) {
               List<Object> list = new ArrayList<>(estimateForOneNode);
               list.add(key);
               requestedKeys.put(distributionInfo.primary(), list);
            }
         } else if (availableKeys != null) {
            availableKeys.add(key);
         }
      }
      return requestedKeys;
   }

   protected Object wrapFunctionalManyResultOnNonOrigin(InvocationContext rCtx, Collection<?> keys, Object[] values) {
      return values;
   }

   protected Object[] unwrapFunctionalManyResultOnOrigin(InvocationContext ctx, List<Object> keys, Object responseValue) {
      return responseValue instanceof Object[] ? (Object[]) responseValue : null;
   }

   protected Response getSingleSuccessfulResponseOrFail(Map<Address, Response> responseMap, CompletableFuture<?> future) {
      Iterator<Response> it = responseMap.values().iterator();
      if (!it.hasNext()) {
         future.completeExceptionally(new RpcException("Expected one response"));
         return null;
      } else {
         Response response = it.next();
         if (it.hasNext()) {
            future.completeExceptionally(new IllegalStateException("Too many responses " + responseMap));
            return null;
         }
         if (!response.isSuccessful()) {
            // CHECKME: The command is sent with current topology and deferred until the node gets our topology;
            // therefore if it returns unsure response we can assume that there is a newer topology
            future.completeExceptionally(new OutdatedTopologyException("Remote node has higher topology, response " + response));
            return null;
         }
         return response;
      }
   }

   protected boolean checkInvocationRecord(InvocationContext context, DataWriteCommand command, DistributionInfo distributionInfo, ByRef<Object> returnValue, BackupHandler handler) {
      CacheEntry cacheEntry = context.lookupEntry(command.getKey());
      Metadata metadata = cacheEntry.getMetadata();
      if (metadata == null) {
         return false;
      }
      InvocationRecord invocationRecord = metadata.invocation(command.getCommandInvocationId());
      // having invocation record implies that the command was successful
      if (invocationRecord == null) {
         return false;
      }
      // If the record is the last invoked command, it is possible that the other owners did not
      // get the change - we must not execute the command locally but we have to replicate it
      // to the other backups
      if (invocationRecord == metadata.lastInvocation()) {
         // We can already set this to authoritative, though we cannot confirm that this will
         // be properly replicated to all backups - originator will fail if it does not get
         // acks from some backups, though
         invocationRecord.setAuthoritative();

         // 1) strict ordering: the command has already been applied, other nodes may have
         // missed it, but we don't know what was the previous invocation *before* the command.
         // 2) delta write needs previous value
         // In any case, replicate full value
         if (command instanceof StrictOrderingCommand || command.hasAnyFlag(FlagBitSets.DELTA_WRITE)) {
            // It would be nicer to use RemoveCommand if the value is null, but RemoveCommand can't carry metadata
            command = new PutKeyValueCommand(cacheEntry.getKey(), cacheEntry.getValue(), false, null,
                  cacheEntry.getMetadata(), (command.getFlagsBitSet() & ~FlagBitSets.DELTA_WRITE) | FlagBitSets.WITH_INVOCATION_RECORDS,
                  command.getCommandInvocationId(), null);
         }
         returnValue.set(handler.invoke(command, distributionInfo, invocationRecord.returnValue));
         // TODO: if setExecuted does not modify flags, we can move it up
         // set no execution flags only after invoking the handler which may cause replication
         command.setCompleted(command.getKey());
         cdl.notifyCommitEntry(invocationRecord.isCreated(), invocationRecord.isRemoved(), false,
               cacheEntry, context, command, null, null);
         return true;

      }
      command.setCompleted(command.getKey());
      returnValue.set(invocationRecord.returnValue);
      return true;
   }

   protected <Item> Map<Object, Object> checkInvocationRecords(InvocationContext ctx, WriteCommand command,
                                                               Collection<Item> items, Consumer<Item> backupConsumer,
                                                               Consumer<Item> fullBackupConsumer,
                                                               WriteManyCommandHelper helper) {
      Map<Object, Object> completedResults = null;
      for (Iterator<Item> iterator = items.iterator(); iterator.hasNext(); ) {
         Item item = iterator.next();
         Object key = helper.item2key(item);
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null) {
            // It is possible that this is executed on backup write-owner, read-non-owner
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            backupConsumer.accept(item);
            continue;
         }
         Metadata metadata = cacheEntry.getMetadata();
         InvocationRecord invocationRecord = metadata == null ? null : metadata.invocation(command.getCommandInvocationId());
         // having invocation record implies that the command was successful
         if (invocationRecord != null) {
            // If the record is the last invoked command, it is possible that the other owners did not
            // get the change - we must not execute the command locally but we have to replicate it
            // to the other backups
            if (invocationRecord == metadata.lastInvocation()) {
               // We can already set this to authoritative, though we cannot confirm that this will
               // be properly replicated to all backups - originator will fail if it does not get
               // acks from some backups, though
               invocationRecord.setAuthoritative();
               if (command instanceof StrictOrderingCommand) {
                  fullBackupConsumer.accept(item);
               } else {
                  backupConsumer.accept(item);
               }
            } else {
               backupConsumer.accept(item);
            }
            if (!command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
               if (completedResults == null) {
                  completedResults = new HashMap<>();
               }
               completedResults.put(key, invocationRecord.returnValue);
            }
            command.setCompleted(key);
            iterator.remove();
            cdl.notifyCommitEntry(invocationRecord.isCreated(), invocationRecord.isRemoved(), false,
                  cacheEntry, ctx, command, null, null);
         } else {
            backupConsumer.accept(item);
         }
      }
      return completedResults;
   }

   protected Object handleUnsuccessfulWriteOnPrimary(InvocationContext ctx, Object rv, Object key, DistributionInfo distributionInfo) {
      CacheEntry cacheEntry = ctx.lookupEntry(key);
      Metadata metadata = cacheEntry.getMetadata();
      InvocationRecord invocationRecord = metadata == null ? null : metadata.lastInvocation();
      if (invocationRecord != null && !invocationRecord.isAuthoritative()) {
         if (trace) log.trace("Replicating full entry to all backups");
         // Before replying, make sure that backup copies are in sync with primary to prevent ISPN-3918
         // TODO: With triangle, ISPN-3918 can happen any time when a command fails since upon unsuccessful execution
         // we don't wait for the replication to backup.
         // don't use provided value, that's already passed in metadata
         PutKeyValueCommand putKeyValueCommand = new PutKeyValueCommand(key, cacheEntry.getValue(), false, null,
               metadata, REPLICATE_NON_AUTHORITATIVE_FLAGS, invocationRecord.id, null);
         return asyncValue(rpcManager.invokeRemotelyAsync(distributionInfo.writeBackups(), putKeyValueCommand, defaultSyncOptions)
               .thenApply(nil -> {
                  invocationRecord.setAuthoritative();
                  return rv;
               }));
      }
      if (trace) log.trace("Skipping the replication of the conditional command as it did not succeed on primary owner.");
      return rv;
   }

   // we're assuming that this function is ran on primary owner of given segments
   protected Map<Address, Set<Integer>> backupOwnersOfSegments(ConsistentHash ch, Set<Integer> segments) {
      Map<Address, Set<Integer>> map = new HashMap<>(ch.getMembers().size());
      if (ch.isReplicated()) {
         for (Address member : ch.getMembers()) {
            map.put(member, segments);
         }
         map.remove(rpcManager.getAddress());
      } else if (ch.getNumOwners() > 1) {
         for (Integer segment : segments) {
            List<Address> owners = ch.locateOwnersForSegment(segment);
            for (int i = 1; i < owners.size(); ++i) {
               map.computeIfAbsent(owners.get(i), o -> new HashSet<>()).add(segment);
            }
         }
      }
      return map;
   }

   @FunctionalInterface
   protected interface BackupHandler {
      Object invoke(DataWriteCommand command, DistributionInfo distributionInfo, Object rv);
   }

   protected static class ArrayIterator {
      private final Object[] array;
      private int pos = 0;

      public ArrayIterator(Object[] array) {
         this.array = array;
      }

      public void add(Object item) {
         array[pos] = item;
         ++pos;
      }

      public void combine(ArrayIterator other) {
         throw new UnsupportedOperationException("The stream is not supposed to be parallel");
      }
   }

   // This class sis very similar to CountDownCompletableFuture but it expect external synchronization,
   // That comes handy when we have to sync anyway on different item, for example the context
   private static class CompletableFutureWithCounter extends CompletableFuture<Void> {
      private int counter;

      public CompletableFutureWithCounter(int counter) {
         this.counter = counter;
      }
   }

   protected static class CountDownCompletableFuture extends CompletableFuture<Object> implements BiConsumer<Object, Throwable> {
      protected final InvocationContext ctx;
      protected final AtomicInteger counter;

      public CountDownCompletableFuture(InvocationContext ctx, int participants) {
         if (trace) log.tracef("Creating shortcut countdown with %d participants", participants);
         this.ctx = ctx;
         assert participants > 0;
         this.counter = new AtomicInteger(participants);
      }

      public void countDown() {
         if (counter.decrementAndGet() == 0) {
            complete(result());
         }
      }

      public void increment() {
         int preValue = counter.getAndIncrement();
         if (preValue == 0) {
            throw new IllegalStateException();
         }
      }

      protected Object result() {
         return null;
      }

      @Override
      public void accept(Object o, Throwable throwable) {
         if (throwable != null) {
            completeExceptionally(throwable);
         } else {
            countDown();
         }
      }
   }

   protected static class MergingCompletableFuture<T> extends CountDownCompletableFuture {
      private final Function<T[], Object> transform;
      protected final T[] results;

      public MergingCompletableFuture(InvocationContext ctx, int participants, T[] results, Function<T[], Object> transform) {
         super(ctx, participants);
         // results can be null if the command has flag IGNORE_RETURN_VALUE
         this.results = results;
         this.transform = transform;
      }

      @Override
      protected Object result() {
         return transform == null || results == null ? null : transform.apply(results);
      }
   }

   private Object visitGetCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      return ctx.lookupEntry(command.getKey()) == null ? onEntryMiss(ctx, command) : invokeNext(ctx, command);
   }

   private Object onEntryMiss(InvocationContext ctx, AbstractDataCommand command) {
      return ctx.isOriginLocal() ?
            handleMissingEntryOnLocalRead(ctx, command) : UnsureResponse.INSTANCE;
   }

   private Object handleMissingEntryOnLocalRead(InvocationContext ctx, AbstractDataCommand command) {
      return readNeedsRemoteValue(command) ?
            asyncInvokeNext(ctx, command, remoteGet(ctx, command.getKey(), false, command.getTopologyId(), command.getFlagsBitSet())) :
            null;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return visitGetCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx,
                                           GetCacheEntryCommand command) throws Throwable {
      return visitGetCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command)
         throws Throwable {
      // TODO: repeatable-reads are not implemented, these need to keep the read values on remote side for the duration
      // of the transaction, and that requires synchronous invocation of the readonly command on all owners.
      // For better consistency, use versioning and write skew check that will fail the transaction when we apply
      // the function on different version of the entry than the one previously read
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null) {
         if (ctx.isOriginLocal()) {
            // the entry is owned locally (it is NullCacheEntry if it was not found), no need to go remote
            return invokeNext(ctx, command);
         } else {
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
                  wrapFunctionalResultOnNonOriginOnReturn(rv, entry));
         }
      }
      if (!ctx.isOriginLocal()) {
         return UnsureResponse.INSTANCE;
      }
      if (readNeedsRemoteValue(command)) {
         LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
         Collection<Address> owners = cacheTopology.getDistribution(key).readOwners();
         if (trace)
            log.tracef("Doing a remote get for key %s in topology %d to %s", key, cacheTopology.getTopologyId(), owners);

         ReadOnlyKeyCommand remoteCommand = remoteReadOnlyCommand(ctx, command);
         // make sure that the command topology is set to the value according which we route it
         remoteCommand.setTopologyId(cacheTopology.getTopologyId());

         CompletableFuture<Map<Address, Response>> rpc = rpcManager.invokeRemotelyAsync(owners, remoteCommand, getStaggeredOptions(owners.size()));
         return asyncValue(rpc.thenApply(responses -> {
               for (Response rsp : responses.values()) {
                  if (rsp.isSuccessful()) {
                     return unwrapFunctionalResultOnOrigin(ctx, key, ((SuccessfulResponse) rsp).getResponseValue());
                  }
               }
               throw handleMissingSuccessfulResponse(responses);
            }));
      } else {
         // This has LOCAL flags, just wrap NullCacheEntry and let the command run
         entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), true, false);
         return invokeNext(ctx, command);
      }
   }

   protected ReadOnlyKeyCommand remoteReadOnlyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) {
      return command;
   }

   protected Object wrapFunctionalResultOnNonOriginOnReturn(Object rv, CacheEntry entry) {
      return rv;
   }

   protected Object unwrapFunctionalResultOnOrigin(InvocationContext ctx, Object key, Object responseValue) {
      return responseValue;
   }

   protected LocalizedCacheTopology checkTopologyId(int topologyId) {
      LocalizedCacheTopology cacheTopology = dm.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      if (currentTopologyId != topologyId && topologyId != -1) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
               topologyId + ", got " + currentTopologyId);
      }
      if (trace) {
         log.tracef("Current topology %d, command topology %d", currentTopologyId, topologyId);
      }
      return cacheTopology;
   }

   /**
    * @return {@code true} if the value is not available on the local node and a read command is allowed to
    * fetch it from a remote node. Does not check if the value is already in the context.
    */
   protected boolean readNeedsRemoteValue(AbstractDataCommand command) {
      return !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP);
   }

   @FunctionalInterface
   protected interface RemoteReadManyCommandBuilder<C> {
      ReplicableCommand build(InvocationContext ctx, C command, List<Object> keys);
   }

   protected interface ReadManyCommandHelper<C> extends InvocationSuccessFunction {
      Collection<?> keys(C command);
      C copyForLocal(C command, List<Object> keys);
      ReplicableCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx);
      void applyLocalResult(MergingCompletableFuture allFuture, Object rv);
      Object transformResult(Object[] results);
   }

   /**
    * Classifies the keys by primary owner (address => keys & segments) and backup owners (address => segments).
    * <p>
    * The first map is used to forward the command to the primary owner with the subset of keys.
    * <p>
    * The second map is used to initialize the {@link CommandAckCollector} to wait for the backups acknowledges.
    */
   protected static class PrimaryOwnerClassifier<Container, Item> {
      protected final Map<Address, Container> primaries;
      private final LocalizedCacheTopology cacheTopology;
      private final WriteManyCommandHelper<?, Container, Item> helper;

      protected PrimaryOwnerClassifier(LocalizedCacheTopology cacheTopology, int entryCount, WriteManyCommandHelper<?, Container, Item> helper) {
         this.cacheTopology = cacheTopology;
         int memberSize = cacheTopology.getMembers().size();
         this.primaries = new HashMap<>(memberSize);
         this.helper = helper;
      }

      public void add(Item item) {
         int segment = cacheTopology.getSegment(helper.item2key(item));
         DistributionInfo distributionInfo = cacheTopology.getDistributionForSegment(segment);
         add(item, distributionInfo);
      }

      protected void add(Item item, DistributionInfo distributionInfo) {
         final Address primaryOwner = distributionInfo.primary();
         Container container = primaries.computeIfAbsent(primaryOwner, address -> helper.newContainer());
         helper.accumulate(container, item);
      }
   }

   protected class ReadOnlyManyHelper implements ReadManyCommandHelper<ReadOnlyManyCommand> {
      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         return wrapFunctionalManyResultOnNonOrigin(rCtx, ((ReadOnlyManyCommand) rCommand).getKeys(), ((Stream) rv).toArray());
      }

      @Override
      public Collection<?> keys(ReadOnlyManyCommand command) {
         return command.getKeys();
      }

      @Override
      public ReadOnlyManyCommand copyForLocal(ReadOnlyManyCommand command, List<Object> keys) {
         return new ReadOnlyManyCommand(command).withKeys(keys);
      }

      @Override
      public ReplicableCommand copyForRemote(ReadOnlyManyCommand command, List<Object> keys, InvocationContext ctx) {
         return new ReadOnlyManyCommand(command).withKeys(keys);
      }

      @Override
      public void applyLocalResult(MergingCompletableFuture allFuture, Object rv) {
         Supplier<ArrayIterator> supplier = () -> new ArrayIterator(allFuture.results);
         BiConsumer<ArrayIterator, Object> consumer = ArrayIterator::add;
         BiConsumer<ArrayIterator, ArrayIterator> combiner = ArrayIterator::combine;
         ((Stream) rv).collect(supplier, consumer, combiner);
      }

      @Override
      public Object transformResult(Object[] results) {
         return Arrays.stream(results);
      }
   }

   protected abstract class WriteManyCommandHelper<C extends WriteCommand, Container, Item> {
      public abstract C copyForPrimary(C cmd, Container items);

      public abstract C copyForBackup(C cmd, Container items);

      public abstract Collection<Item> getItems(C cmd);

      public abstract Object item2key(Item item);

      public abstract Container newContainer();

      public abstract void accumulate(Container container, Item item);

      public abstract Collection<Item> asCollection(Container items);

      public abstract int containerSize(Container container);

      public abstract boolean isForwarded(C cmd);

      public abstract Object transformResult(Object[] results);

      public abstract Object transformResult(Object key, Object result);

      public abstract Object mergeResults(Object rv, Map<Object, Object> results);
   }

   private class PutMapHelper extends WriteManyCommandHelper<PutMapCommand, Map<Object, Object>, Map.Entry<Object, Object>> {
      @Override
      public PutMapCommand copyForPrimary(PutMapCommand cmd, Map<Object, Object> items) {
         return new PutMapCommand(cmd).withMap(items);
      }

      @Override
      public PutMapCommand copyForBackup(PutMapCommand cmd, Map<Object, Object> items) {
         PutMapCommand copy = new PutMapCommand(cmd).withMap(items);
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Map.Entry<Object, Object>> getItems(PutMapCommand cmd) {
         return cmd.getMap().entrySet();
      }

      @Override
      public Object item2key(Map.Entry<Object, Object> entry) {
         return entry.getKey();
      }

      @Override
      public Map<Object, Object> newContainer() {
         return new HashMap<>();
      }

      @Override
      public void accumulate(Map<Object, Object> map, Map.Entry<Object, Object> entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public Collection<Map.Entry<Object, Object>> asCollection(Map<Object, Object> items) {
         return items.entrySet();
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean isForwarded(PutMapCommand cmd) {
         return cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         if (results == null) return null;
         Map<Object, Object> result = new HashMap<>();
         for (Object r : results) {
            Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) r;
            result.put(entry.getKey(), entry.getValue());
         }
         return result;
      }

      @Override
      public Object transformResult(Object key, Object result) {
         return Immutables.immutableEntry(key, result);
      }

      @Override
      public Object mergeResults(Object rv, Map<Object, Object> results) {
         if (rv != null) {
            ((Map<Object, Object>) rv).putAll(results);
         }
         return rv;
      }
   }
}
