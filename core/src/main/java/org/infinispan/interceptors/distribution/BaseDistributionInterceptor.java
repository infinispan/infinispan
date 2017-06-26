package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
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
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
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
import org.infinispan.remoting.RemoteException;
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
   private static final Object LOST_PLACEHOLDER = new Object();

   protected DistributionManager dm;
   protected RemoteValueRetrievedListener rvrl;
   protected KeyPartitioner keyPartitioner;

   protected boolean isL1Enabled;
   protected boolean isReplicated;

   private final ReadOnlyManyHelper readOnlyManyHelper = new ReadOnlyManyHelper();
   private final InvocationSuccessFunction primaryReturnHandler = this::primaryReturnHandler;

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager, RemoteValueRetrievedListener rvrl,
                                  KeyPartitioner keyPartitioner) {
      this.dm = distributionManager;
      this.rvrl = rvrl;
      this.keyPartitioner = keyPartitioner;
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
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      Address primaryOwner = dm.getCacheTopology().getDistribution(command.getGroupName()).primary();
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

   protected <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletableFuture<Void> remoteGet(
         InvocationContext ctx, C command, Object key, boolean isWrite) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      int topologyId = cacheTopology.getTopologyId();

      DistributionInfo info = cacheTopology.getDistribution(key);
      if (info.isReadOwner()) {
         if (trace) {
            log.tracef("Key %s became local after wrapping, retrying command. Command topology is %d, current topology is %d",
                  key, command.getTopologyId(), topologyId);
         }
         // The topology has changed between EWI and BDI, let's retry
         if (command.getTopologyId() == topologyId) {
            throw new IllegalStateException();
         }
         throw new OutdatedTopologyException(topologyId);
      }
      if (trace) {
         log.tracef("Perform remote get for key %s. currentTopologyId=%s, owners=%s",
            key, topologyId, info.readOwners());
      }

      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, command.getFlagsBitSet());
      getCommand.setTopologyId(topologyId);
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
      CacheEntry entry = ctx.lookupEntry(key);

      if (isLocalModeForced(command)) {
         if (entry == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo info = cacheTopology.getDistribution(key);
      if (entry == null) {
         boolean load = shouldLoad(ctx, command, info);
         if (info.isPrimary()) {
            throw new IllegalStateException("Primary owner in writeCH should always be an owner in readCH as well.");
         } else if (ctx.isOriginLocal()) {
            return invokeRemotely(command, info.primary());
         } else {
            if (load) {
               CompletableFuture<?> getFuture = remoteGet(ctx, command, command.getKey(), true);
               return asyncInvokeNext(ctx, command, getFuture);
            } else {
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               return invokeNext(ctx, command);
            }
         }
      } else {
         if (info.isPrimary()) {
            return invokeNextThenApply(ctx, command, primaryReturnHandler);
         } else if (ctx.isOriginLocal()) {
            return invokeRemotely(command, info.primary());
         } else {
            return invokeNext(ctx, command);
         }
      }
   }

   private boolean shouldLoad(InvocationContext ctx, AbstractDataWriteCommand command, DistributionInfo info) {
      if (!command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         VisitableCommand.LoadType loadType = command.loadType();
         switch (loadType) {
            case DONT_LOAD:
               return false;
            case OWNER:
               return info.isPrimary() || (info.isWriteOwner() && !ctx.isOriginLocal());
            case PRIMARY:
               return info.isPrimary();
            default:
               throw new IllegalStateException();
         }
      } else {
         return false;
      }
   }

   private Object invokeRemotely(DataWriteCommand command, Address primaryOwner) {
      if (trace) log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
      boolean isSyncForwarding = isSynchronous(command) || command.isReturnValueExpected();

      CompletableFuture<Map<Address, Response>> remoteInvocation;
      try {
         remoteInvocation = rpcManager.invokeRemotelyAsync(Collections.singletonList(primaryOwner), command,
               isSyncForwarding ? defaultSyncOptions : defaultAsyncOptions);
      } catch (Throwable t) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
         throw t;
      }
      if (isSyncForwarding) {
         return asyncValue(remoteInvocation.handle((responses, t) -> {
            command.setValueMatcher(command.getValueMatcher().matcherForRetry());
            CompletableFutures.rethrowException(t);

            Response response = getSingleResponse(responses);
            if (!response.isSuccessful()) {
               command.fail();
            } else if (!(response instanceof ValidResponse)) {
               throw unexpected(response);
            }
            // We expect only successful/unsuccessful responses, not unsure
            return ((ValidResponse) response).getResponseValue();
         }));
      } else {
         return null;
      }
   }

   private Object primaryReturnHandler(InvocationContext ctx, VisitableCommand visitableCommand, Object localResult) {
      DataWriteCommand command = (DataWriteCommand) visitableCommand;
      if (!command.isSuccessful()) {
         if (trace) log.tracef("Skipping the replication of the conditional command as it did not succeed on primary owner (%s).", command);
         return localResult;
      }
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo distributionInfo = cacheTopology.getDistribution(command.getKey());
      Collection<Address> owners = distributionInfo.writeOwners();
      if (owners.size() == 1) {
         // There are no backups, skip the replication part.
         return localResult;
      }
      Collection<Address> recipients = isReplicated ? null : owners;

      // Cache the matcher and reset it if we get OOTE (or any other exception) from backup
      ValueMatcher originalMatcher = command.getValueMatcher();
      // Ignore the previous value on the backup owners
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      RpcOptions rpcOptions = determineRpcOptionsForBackupReplication(rpcManager, isSynchronous(command), recipients);
      CompletableFuture<Map<Address, Response>> remoteInvocation =
            rpcManager.invokeRemotelyAsync(recipients, command, rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
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

      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, command.getKeys(), cacheTopology, null, null);
      if (requestedKeys.isEmpty()) {
         return invokeNext(ctx, command);
      }

      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      ClusteredGetAllFuture allFuture = new ClusteredGetAllFuture(requestedKeys.size(), command);

      for (Map.Entry<Address, List<Object>> pair : requestedKeys.entrySet()) {
         ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(pair.getValue(), command.getFlagsBitSet(), gtx);
         clusteredGetAllCommand.setTopologyId(command.getTopologyId());
         rpcManager.invokeRemotelyAsync(Collections.singleton(pair.getKey()), clusteredGetAllCommand, syncIgnoreLeavers)
               .whenComplete(new ClusteredGetAllHandler(pair.getKey(), allFuture, ctx, command, pair.getValue(), null));
      }
      return asyncValue(allFuture).thenApply(ctx, command, allFuture);
   }

   protected void handleRemotelyRetrievedKeys(InvocationContext ctx, List<?> remoteKeys) {
   }

   private class ClusteredGetAllHandler implements BiConsumer<Map<Address, Response>, Throwable> {
      private final Address target;
      private final ClusteredGetAllFuture allFuture;
      private final InvocationContext ctx;
      private final GetAllCommand command;
      private final List<?> keys;
      private final Map<Object, Collection<Address>> contactedNodes;

      private ClusteredGetAllHandler(Address target, ClusteredGetAllFuture allFuture, InvocationContext ctx,
                                     GetAllCommand command, List<?> keys, Map<Object, Collection<Address>> contactedNodes) {
         this.target = target;
         this.allFuture = allFuture;
         this.keys = keys;
         this.ctx = ctx;
         this.command = command;
         this.contactedNodes = contactedNodes;
      }

      @Override
      public void accept(Map<Address, Response> responseMap, Throwable throwable) {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
            return;
         }
         SuccessfulResponse response = getSuccessfulResponseOrFail(responseMap, allFuture, this::handleMissingResponse);
         if (response == null) {
            return;
         }
         Object responseValue = response.getResponseValue();
         if (!(responseValue instanceof InternalCacheValue[])) {
            allFuture.completeExceptionally(new IllegalStateException("Unexpected response value: " + responseValue));
            return;
         }
         InternalCacheValue[] values = (InternalCacheValue[]) responseValue;
         synchronized (allFuture) {
            // Check if other handlers haven't finished with an exception
            if (allFuture.isDone()) {
               return;
            }
            for (int i = 0; i < keys.size(); ++i) {
               Object key = keys.get(i);
               InternalCacheValue value = values[i];
               CacheEntry entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
               wrapRemoteEntry(ctx, key, entry, false);
            }
            handleRemotelyRetrievedKeys(ctx, keys);
            if (--allFuture.counter == 0) {
               allFuture.complete(null);
            }
         }
      }

      private void handleMissingResponse(Response response) {
         if (response instanceof UnsureResponse) {
            allFuture.hasUnsureResponse = true;
         }
         GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;

         Map<Object, Collection<Address>> contactedNodes = this.contactedNodes == null ? new HashMap<>() : this.contactedNodes;
         Map<Address, List<Object>> requestedKeys;
         synchronized (contactedNodes) {
            for (Object key : keys) {
               contactedNodes.computeIfAbsent(key, k -> new ArrayList<>(4)).add(target);
            }
            requestedKeys = getKeysByOwner(ctx, keys, checkTopologyId(command), null, contactedNodes);
         }

         synchronized (allFuture) {
            allFuture.counter += requestedKeys.size();
         }
         for (Map.Entry<Address, List<Object>> pair : requestedKeys.entrySet()) {
            ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(pair.getValue(), command.getFlagsBitSet(), gtx);
            clusteredGetAllCommand.setTopologyId(command.getTopologyId());
            // Note that keys here are only the subset of keys requested from the node which did not send a valid response
            keys.removeAll(pair.getValue());
            rpcManager.invokeRemotelyAsync(Collections.singleton(pair.getKey()), clusteredGetAllCommand, syncIgnoreLeavers)
                  .whenComplete(new ClusteredGetAllHandler(pair.getKey(), allFuture, ctx, command, pair.getValue(), contactedNodes));
         }
         if (!keys.isEmpty()) {
            // GetAllCommand requires all keys to be wrapped when it comes to execute perform() methods, therefore
            // we need to remove those for which we have not received any entry
            synchronized (allFuture) {
               Set<?> strippedKeys = new HashSet<>(allFuture.localCommand.getKeys());
               strippedKeys.removeAll(keys);
               // We can't just call command.setKeys() - interceptors might compare keys and actual result set
               allFuture.localCommand = cf.buildGetAllCommand(strippedKeys, command.getFlagsBitSet(), command.isReturnEntries());
               allFuture.lostData = true;
            }
         }
         synchronized (allFuture) {
            if (--allFuture.counter == 0) {
               allFuture.complete(null);
            }
         }
      }
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

      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
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
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, keys, cacheTopology, availableKeys, null);

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
         ReplicableCommand remoteCommand = helper.copyForRemote(command, keysForAddress, ctx);
         Set<Address> target = Collections.singleton(addressKeys.getKey());
         rpcManager.invokeRemotelyAsync(target, remoteCommand, syncIgnoreLeavers)
            .whenComplete(new ReadManyHandler(addressKeys.getKey(), allFuture, ctx, command, keysForAddress, null, pos, helper));
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

   private class ReadManyHandler<C extends TopologyAffectedCommand> implements BiConsumer<Map<Address, Response>, Throwable> {
      private final Address target;
      private final MergingCompletableFuture<Object> allFuture;
      private final InvocationContext ctx;
      private final C command;
      private final List<Object> keys;
      private final int destinationIndex;
      private final Map<Object, Collection<Address>> contactedNodes;
      private final ReadManyCommandHelper<C> helper;;

      private ReadManyHandler(Address target, MergingCompletableFuture<Object> allFuture, InvocationContext ctx, C command, List<Object> keys,
                              Map<Object, Collection<Address>> contactedNodes, int destinationIndex, ReadManyCommandHelper<C> helper) {
         this.target = target;
         this.allFuture = allFuture;
         this.ctx = ctx;
         this.command = command;
         this.keys = keys;
         this.destinationIndex = destinationIndex;
         this.contactedNodes = contactedNodes;
         this.helper = helper;
      }

      @Override
      public void accept(Map<Address, Response> responseMap, Throwable throwable) {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
            return;
         }
         SuccessfulResponse response = getSuccessfulResponseOrFail(responseMap, allFuture, this::handleMissingResponse);
         if (response == null) {
            return;
         }
         try {
            Object responseValue = response.getResponseValue();
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
      }

      private void handleMissingResponse(Response response) {
         if (response instanceof UnsureResponse) {
            allFuture.hasUnsureResponse = true;
         }
         Map<Object, Collection<Address>> contactedNodes = this.contactedNodes == null ? new HashMap<>() : this.contactedNodes;
         Map<Address, List<Object>> requestedKeys;
         synchronized (contactedNodes) {
            for (Object key : keys) {
               contactedNodes.computeIfAbsent(key, k -> new ArrayList<>(4)).add(target);
            }
            requestedKeys = getKeysByOwner(ctx, keys, checkTopologyId(command), null, contactedNodes);
         }
         int pos = destinationIndex;
         for (Map.Entry<Address, List<Object>> addressKeys : requestedKeys.entrySet()) {
            allFuture.increment();
            List<Object> keysForAddress = addressKeys.getValue();
            ReplicableCommand remoteCommand = helper.copyForRemote(command, keysForAddress, ctx);
            Set<Address> target = Collections.singleton(addressKeys.getKey());
            rpcManager.invokeRemotelyAsync(target, remoteCommand, syncIgnoreLeavers)
                  .whenComplete(new ReadManyHandler(addressKeys.getKey(), allFuture, ctx, command, keysForAddress, contactedNodes, pos, helper));
            pos += keysForAddress.size();
         }
         Arrays.fill(allFuture.results, pos, destinationIndex + keys.size(), LOST_PLACEHOLDER);
         allFuture.lostData = true;
         allFuture.countDown();
      }
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
                                                     List<Object> availableKeys,
                                                     Map<Object, Collection<Address>> ignoredOwners) {
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
            Collection<Address> ignoreForKey = null;
            for (Address address : distributionInfo.readOwners()) {
               if (address.equals(rpcManager.getAddress())) {
                  throw new IllegalStateException("Entry should be always wrapped!");
               } else if (ignoredOwners != null) {
                  if (ignoreForKey == null) {
                     ignoreForKey = ignoredOwners.get(key);
                  }
                  if (ignoreForKey != null && ignoreForKey.contains(address)) {
                     continue;
                  }
               }
               List<Object> list = requestedKeys.get(address);
               if (list != null) {
                  list.add(key);
                  foundExisting = true;
                  break;
               }
            }
            if (!foundExisting) {
               Address target = null;
               if (ignoredOwners == null) {
                  target = distributionInfo.primary();
               } else {
                  for (Address address : distributionInfo.readOwners()) {
                     if (ignoreForKey == null) {
                        ignoreForKey = ignoredOwners.get(key);
                     }
                     if (ignoreForKey == null || !ignoreForKey.contains(address)) {
                        target = address;
                        break;
                     }
                  }
               }
               // If all read owners should be ignored we won't put that entry anywhere
               if (target != null) {
                  List<Object> list = new ArrayList<>(estimateForOneNode);
                  list.add(key);
                  requestedKeys.put(target, list);
               }
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

   protected static class CountDownCompletableFuture extends CompletableFuture<Object> {
      protected final InvocationContext ctx;
      protected final AtomicInteger counter;

      public CountDownCompletableFuture(InvocationContext ctx, int participants) {
         if (trace) log.tracef("Creating shortcut countdown with %d participants", participants);
         this.ctx = ctx;
         this.counter = new AtomicInteger(participants);
      }

      public void countDown() {
         if (counter.decrementAndGet() == 0) {
            Object result = null;
            try {
               result = result();
            } catch (Throwable t) {
               completeExceptionally(t);
            } finally {
               // no-op when completed with exception
               complete(result);
            }
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
   }

   protected static class MergingCompletableFuture<T> extends CountDownCompletableFuture {
      private final Function<T[], Object> transform;
      protected final T[] results;
      protected volatile boolean hasUnsureResponse;
      protected volatile boolean lostData;

      public MergingCompletableFuture(InvocationContext ctx, int participants, T[] results, Function<T[], Object> transform) {
         super(ctx, participants);
         // results can be null if the command has flag IGNORE_RETURN_VALUE
         this.results = results;
         this.transform = transform;
      }

      @Override
      protected Object result() {
         // If we've lost data but did not get any unsure responses we should return limited stream.
         // If we've got unsure response but did not lose any data - no problem, there has been another
         // response delivering the results.
         // Only if those two combine we'll rather throw OTE and retry.
         if (hasUnsureResponse && lostData) {
            throw OutdatedTopologyException.INSTANCE;
         }
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
            asyncInvokeNext(ctx, command, remoteGet(ctx, command, command.getKey(), false)) :
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
         LocalizedCacheTopology cacheTopology = checkTopologyId(command);
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

   protected LocalizedCacheTopology checkTopologyId(TopologyAffectedCommand command) {
      LocalizedCacheTopology cacheTopology = dm.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (currentTopologyId != cmdTopology && cmdTopology != -1) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
            cmdTopology + ", got " + currentTopologyId);
      }
      if (trace) {
         log.tracef("Current topology %d, command topology %d", currentTopologyId, cmdTopology);
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

   protected interface ReadManyCommandHelper<C> extends InvocationSuccessFunction {
      Collection<?> keys(C command);
      C copyForLocal(C command, List<Object> keys);
      ReplicableCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx);
      void applyLocalResult(MergingCompletableFuture allFuture, Object rv);
      Object transformResult(Object[] results);
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
         return Arrays.stream(results).filter(o -> o != LOST_PLACEHOLDER);
      }
   }

}
