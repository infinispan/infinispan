package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.remote.expiration.UpdateLastAccessCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ArrayCollector;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.RemoteGetResponseCollector;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * Base class for distribution of entries across a cluster.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 */
public abstract class BaseDistributionInterceptor extends ClusteringInterceptor {
   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Object LOST_PLACEHOLDER = new Object();

   @Inject protected RemoteValueRetrievedListener rvrl;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected TimeService timeService;
   @Inject protected InternalExpirationManager<Object, Object> expirationManager;

   protected boolean isL1Enabled;
   protected boolean isReplicated;

   private final ReadOnlyManyHelper readOnlyManyHelper = new ReadOnlyManyHelper();
   private final InvocationSuccessFunction primaryReturnHandler = this::primaryReturnHandler;

   @Override
   protected Log getLog() {
      return log;
   }

   @Start
   public void configure() {
      // Can't rely on the super injectConfiguration() to be called before our injectDependencies() method2
      isL1Enabled = cacheConfiguration.clustering().l1().enabled();
      isReplicated = cacheConfiguration.clustering().cacheMode().isReplicated();
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      if (isReplicated) {
         // Replicated size command has no reason to be distributed as we do is count entries, no processing
         // done upon these entries and the overhead of coordinating remote nodes and network calls is more expensive
         command.setFlagsBitSet(command.getFlagsBitSet() | FlagBitSets.CACHE_MODE_LOCAL);
      }
      return super.visitSizeCommand(ctx, command);
   }

   @Override
   public final Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      Address primaryOwner = distributionManager.getCacheTopology().getDistribution(command.getGroupName()).primary();
      CompletionStage<ValidResponse> future = rpcManager.invokeCommand(primaryOwner, command,
                                                                       SingleResponseCollector.validOnly(),
                                                                       rpcManager.getSyncRpcOptions());
      return asyncInvokeNext(ctx, command, future.thenAccept(response -> {
         if (response instanceof SuccessfulResponse) {
            //noinspection unchecked
            List<CacheEntry> cacheEntries = (List<CacheEntry>) response.getResponseValue();
            for (CacheEntry entry : cacheEntries) {
               wrapRemoteEntry(ctx, entry.getKey(), entry, false);
            }
         }
      }));
   }

   @Override
   public final Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         if (isSynchronous(command)) {
            RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
            return asyncInvokeNext(ctx, command,
                                   rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(),
                                                                 rpcOptions));
         } else {
            rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
            return invokeNext(ctx, command);
         }
      }
      return invokeNext(ctx, command);
   }

   protected DistributionInfo retrieveDistributionInfo(LocalizedCacheTopology topology, ReplicableCommand command, Object key) {
      return topology.getSegmentDistribution(SegmentSpecificCommand.extractSegment(command, key, keyPartitioner));
   }

   protected <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletionStage<Void> remoteGet(
         InvocationContext ctx, C command, Object key, boolean isWrite) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      int topologyId = cacheTopology.getTopologyId();

      DistributionInfo info = retrieveDistributionInfo(cacheTopology, command, key);
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

      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, info.segmentId(), command.getFlagsBitSet());
      getCommand.setTopologyId(topologyId);
      getCommand.setWrite(isWrite);

      return rpcManager.invokeCommandStaggered(info.readOwners(), getCommand, new RemoteGetResponseCollector(),
                                               rpcManager.getSyncRpcOptions())
                       .thenAccept(r -> {
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
                          throw handleMissingSuccessfulResponse(r);
                       });
   }

   protected static CacheException handleMissingSuccessfulResponse(Response response) {
      // The response map does not contain any ExceptionResponses; these are rethrown as exceptions
      if (response instanceof UnsureResponse) {
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
            return invokeRemotely(ctx, command, info.primary());
         } else {
            if (load) {
               CompletionStage<?> getFuture = remoteGet(ctx, command, command.getKey(), true);
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
            return invokeRemotely(ctx, command, info.primary());
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

   protected LocalizedCacheTopology checkTopologyId(TopologyAffectedCommand command) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (command instanceof FlagAffectedCommand && ((((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL)))) {
         getLog().tracef("Skipping topology check for command %s", command);
         return cacheTopology;
      }
      if (trace) {
         getLog().tracef("Current topology %d, command topology %d", currentTopologyId, cmdTopology);
      }
      if (cmdTopology >= 0 && currentTopologyId != cmdTopology) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
               cmdTopology + ", got " + currentTopologyId);
      }
      return cacheTopology;
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
      // Cache the matcher and reset it if we get OOTE (or any other exception) from backup
      ValueMatcher originalMatcher = command.getValueMatcher();
      // Ignore the previous value on the backup owners
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      if (!isSynchronous(command)) {
         if (isReplicated) {
            rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
         } else {
            rpcManager.sendToMany(owners, command, DeliverOrder.PER_SENDER);
         }
         // Switch to the retry policy, in case the primary owner changes before we commit locally
         command.setValueMatcher(originalMatcher.matcherForRetry());
         return localResult;
      }
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(isReplicated, owners.size());
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<Map<Address, Response>> remoteInvocation = isReplicated ?
            rpcManager.invokeCommandOnAll(command, collector, rpcOptions) :
            rpcManager.invokeCommand(owners, command, collector, rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
         CompletableFutures.rethrowException(t instanceof RemoteException ? t.getCause() : t);
         return localResult;
      }));
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         for (Object key : command.getKeys()) {
            if (ctx.lookupEntry(key) == null) {
               entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), true, false);
            }
         }
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
      GetAllSuccessHandler getAllSuccessHandler = new GetAllSuccessHandler(command);
      CompletableFuture<Void> allFuture = remoteGetAll(ctx, command, command.getKeys(), getAllSuccessHandler);
      return asyncValue(allFuture).thenApply(ctx, command, getAllSuccessHandler);
   }

   protected <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletableFuture<Void> remoteGetAll(
         InvocationContext ctx, C command, Collection<?> keys, RemoteGetAllHandler remoteGetAllHandler) {
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, keys, checkTopologyId(command), null, null);
      if (requestedKeys.isEmpty()) {
         return CompletableFutures.completedNull();
      }

      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      ClusteredGetAllFuture allFuture = new ClusteredGetAllFuture(requestedKeys.size());

      for (Map.Entry<Address, List<Object>> pair : requestedKeys.entrySet()) {
         ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(pair.getValue(), command.getFlagsBitSet(), gtx);
         clusteredGetAllCommand.setTopologyId(command.getTopologyId());
         Address target = pair.getKey();
         rpcManager.invokeCommand(target, clusteredGetAllCommand, SingletonMapResponseCollector.ignoreLeavers(),
                                  rpcManager.getSyncRpcOptions())
                   .whenComplete(new ClusteredGetAllHandler(target, allFuture, ctx, command, pair.getValue(), null, remoteGetAllHandler));
      }
      return allFuture;
   }

   protected void handleRemotelyRetrievedKeys(InvocationContext ctx, WriteCommand appliedCommand, List<?> remoteKeys) {
   }

   private class ClusteredGetAllHandler<C extends FlagAffectedCommand & TopologyAffectedCommand> implements BiConsumer<Map<Address, Response>, Throwable> {
      private final Address target;
      private final ClusteredGetAllFuture allFuture;
      private final InvocationContext ctx;
      private final C command;
      private final List<?> keys;
      private final Map<Object, Collection<Address>> contactedNodes;
      private final RemoteGetAllHandler remoteGetAllHandler;

      private ClusteredGetAllHandler(Address target, ClusteredGetAllFuture allFuture, InvocationContext ctx,
                                     C command, List<?> keys, Map<Object, Collection<Address>> contactedNodes,
                                     RemoteGetAllHandler remoteGetAllHandler) {
         this.target = target;
         this.allFuture = allFuture;
         this.keys = keys;
         this.ctx = ctx;
         this.command = command;
         this.contactedNodes = contactedNodes;
         this.remoteGetAllHandler = remoteGetAllHandler;
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
         // Check if other handlers haven't finished with an exception, without blocking if the exception is currently
         // being processed in the interceptor stack callbacks.
         if (allFuture.isDone()) {
            return;
         }
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
            handleRemotelyRetrievedKeys(ctx, command instanceof WriteCommand ? (WriteCommand) command : null, keys);
            if (--allFuture.counter == 0) {
               allFuture.complete(null);
            }
         }
      }

      private void handleMissingResponse(Response response) {
         if (response instanceof UnsureResponse) {
            remoteGetAllHandler.onUnsureResponse();
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
            Address target = pair.getKey();
            rpcManager.invokeCommand(target, clusteredGetAllCommand, SingletonMapResponseCollector.ignoreLeavers(),
                                     rpcManager.getSyncRpcOptions())
                      .whenComplete(new ClusteredGetAllHandler(target, allFuture, ctx, command, pair.getValue(),
                                                               contactedNodes, remoteGetAllHandler));
         }
         if (!keys.isEmpty()) {
            synchronized (allFuture) {
               try {
                  remoteGetAllHandler.onKeysLost(keys);
               } catch (Throwable t) {
                  allFuture.completeExceptionally(t);
               }
            }
         }
         synchronized (allFuture) {
            if (--allFuture.counter == 0) {
               allFuture.complete(null);
            }
         }
      }
   }

   protected interface RemoteGetAllHandler {
      void onUnsureResponse();
      void onKeysLost(Collection<?> lostKeys);
   }

   private class GetAllSuccessHandler implements RemoteGetAllHandler, InvocationSuccessFunction {
      private GetAllCommand localCommand;
      private boolean lostData;
      private boolean hasUnsureResponse;

      public GetAllSuccessHandler(GetAllCommand localCommand) {
         this.localCommand = localCommand;
      }

      @Override
      public void onUnsureResponse() {
         hasUnsureResponse = true;
      }

      @GuardedBy("allFuture") // This handler is executed within a synchronized (allFuture) { ... }
      @Override
      public void onKeysLost(Collection<?> lostKeys) {
         // GetAllCommand requires all keys to be wrapped when it comes to execute perform() methods, therefore
         // we need to remove those for which we have not received any entry
         lostData = true;
         Set<?> strippedKeys = new HashSet<>(localCommand.getKeys());
         strippedKeys.removeAll(lostKeys);
         // We can't just call command.setKeys() - interceptors might compare keys and actual result set
         localCommand = cf.buildGetAllCommand(strippedKeys, localCommand.getFlagsBitSet(), localCommand.isReturnEntries());
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         assert rv == null; // value with which the allFuture has been completed
         if (hasUnsureResponse && lostData) {
            throw OutdatedTopologyException.INSTANCE;
         }
         return invokeNext(rCtx, localCommand);
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

      CompletableFuture<Void> requiredKeysFuture = helper.fetchRequiredKeys(cacheTopology, requestedKeys, availableKeys, ctx, command);
      if (requiredKeysFuture == null || requiredKeysFuture.isDone()) {
         return asyncValue(fetchAndApplyValues(ctx, command, helper, keys, availableKeys, requestedKeys));
      } else {
         // We need to run the requiredKeysFuture and fetchAndApplyValues futures serially for two reasons
         // a) adding the values to context after fetching the values remotely is not synchronized
         // b) fetchAndApplyValues invokes the command on availableKeys and stores the result
         return asyncValue(requiredKeysFuture.thenCompose(nil -> fetchAndApplyValues(ctx, command, helper, keys, availableKeys, requestedKeys)));
      }
   }

   private <C extends TopologyAffectedCommand & FlagAffectedCommand> MergingCompletableFuture<Object> fetchAndApplyValues(InvocationContext ctx, C command, ReadManyCommandHelper<C> helper, Collection<?> keys, List<Object> availableKeys, Map<Address, List<Object>> requestedKeys) {
      // TODO: while this works in a non-blocking way, the returned stream is not lazy as the functional
      // contract suggests. Traversable is also not honored as it is executed only locally on originator.
      // On FutureMode.ASYNC, there should be one command per target node going from the top level
      // to allow retries in StateTransferInterceptor in case of topology change.
      MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(
            requestedKeys.size() + (availableKeys.isEmpty() ? 0 : 1),
            new Object[keys.size()], helper::transformResult);

      handleLocallyAvailableKeys(ctx, command, availableKeys, allFuture, helper);
      int pos = availableKeys.size();
      for (Map.Entry<Address, List<Object>> addressKeys : requestedKeys.entrySet()) {
         List<Object> keysForAddress = addressKeys.getValue();

         ReadOnlyManyCommand remoteCommand = helper.copyForRemote(command, keysForAddress, ctx);
         remoteCommand.setTopologyId(command.getTopologyId());
         Address target = addressKeys.getKey();
         rpcManager.invokeCommand(target, remoteCommand, SingletonMapResponseCollector.ignoreLeavers(),
                                  rpcManager.getSyncRpcOptions())
                   .whenComplete(
                         new ReadManyHandler(target, allFuture, ctx, command, keysForAddress, null, pos, helper));
         pos += keysForAddress.size();
      }
      return allFuture;
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

   private class ReadManyHandler<C extends FlagAffectedCommand & TopologyAffectedCommand> implements BiConsumer<Map<Address, Response>, Throwable> {
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
            ReadOnlyManyCommand remoteCommand = helper.copyForRemote(command, keysForAddress, ctx);
            remoteCommand.setTopologyId(command.getTopologyId());
            Address target = addressKeys.getKey();
            rpcManager.invokeCommand(target, remoteCommand, SingletonMapResponseCollector.ignoreLeavers(),
                                     rpcManager.getSyncRpcOptions())
                      .whenComplete(new ReadManyHandler(target, allFuture, ctx, command, keysForAddress,
                                                        contactedNodes, pos, helper));
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

         CompletionStage<Response> rpc =
               rpcManager.invokeCommand(owners, remoteCommand, new RemoteGetResponseCollector(),
                                        rpcManager.getSyncRpcOptions());
         return asyncValue(rpc.thenApply(rsp -> {
            if (rsp.isSuccessful()) {
               return unwrapFunctionalResultOnOrigin(ctx, key, ((SuccessfulResponse) rsp).getResponseValue());
            }
            throw handleMissingSuccessfulResponse(rsp);
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

   protected Object invokeRemotely(InvocationContext ctx, DataWriteCommand command, Address primaryOwner) {
      if (trace) getLog().tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
      boolean isSyncForwarding = isSynchronous(command) || command.isReturnValueExpected();

      if (!isSyncForwarding) {
         rpcManager.sendTo(primaryOwner, command, DeliverOrder.PER_SENDER);
         return null;
      }
      CompletionStage<ValidResponse> remoteInvocation;
      try {
         remoteInvocation = rpcManager.invokeCommand(primaryOwner, command, SingleResponseCollector.validOnly(),
               rpcManager.getSyncRpcOptions());
      } catch (Throwable t) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
         throw t;
      }
      return asyncValue(remoteInvocation).andHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         dataWriteCommand.setValueMatcher(dataWriteCommand.getValueMatcher().matcherForRetry());
         CompletableFutures.rethrowException(t);

         Response response = ((Response) rv);
         if (!response.isSuccessful()) {
            dataWriteCommand.fail();
            // FIXME A response cannot be successful and not valid
         } else if (!(response instanceof ValidResponse)) {
            throw unexpected(response);
         }
         // We expect only successful/unsuccessful responses, not unsure
         return ((ValidResponse) response).getResponseValue();
      });
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
      ReadOnlyManyCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx);
      void applyLocalResult(MergingCompletableFuture allFuture, Object rv);
      Object transformResult(Object[] results);
      CompletableFuture<Void> fetchRequiredKeys(LocalizedCacheTopology cacheTopology, Map<Address, List<Object>> requestedKeys, List<Object> availableKeys, InvocationContext ctx, C command);
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
      public ReadOnlyManyCommand copyForRemote(ReadOnlyManyCommand command, List<Object> keys, InvocationContext ctx) {
         return new ReadOnlyManyCommand(command).withKeys(keys);
      }

      @Override
      public void applyLocalResult(MergingCompletableFuture allFuture, Object rv) {
         ((Stream) rv).collect(new ArrayCollector(allFuture.results));
      }

      @Override
      public Object transformResult(Object[] results) {
         return Arrays.stream(results).filter(o -> o != LOST_PLACEHOLDER);
      }

      @Override
      public CompletableFuture<Void> fetchRequiredKeys(LocalizedCacheTopology cacheTopology, Map<Address, List<Object>> requestedKeys, List<Object> availableKeys, InvocationContext ctx, ReadOnlyManyCommand command) {
         return null;
      }
   }

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      // Lifespan expiration just behaves like a remove command
      if (!command.isMaxIdle()) {
         return visitRemoveCommand(ctx, command);
      }

      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);

      if (isLocalModeForced(command)) {
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      int segment = command.getSegment();
      DistributionInfo info = cacheTopology.getSegmentDistribution(segment);
      if (entry == null) {
         if (info.isPrimary()) {
            throw new IllegalStateException("Primary owner in writeCH should always be an owner in readCH as well.");
         } else if (ctx.isOriginLocal()) {
            // Primary has to handle max idle removal
            return invokeRemotely(ctx, command, info.primary());
         } else {
            throw new IllegalStateException("Non primary owner recipient of remote remove expired command.");
         }
      } else {
         if (info.isPrimary()) {
            // We don't pass the value for performance as we already have the lock obtained for this key - so it can't
            // change from its current value
            CompletableFuture<Long> completableFuture = expirationManager.retrieveLastAccess(key, null, segment);

            return asyncValue(completableFuture).thenApply(ctx, command, (rCtx, rCommand, max) -> {
               if (max != null) {
                  // Make sure to fail the command for other interceptors, such as CacheWriterInterceptor, so it
                  // won't remove it in the store
                  command.fail();
                  if (trace) {
                     log.tracef("Received %s as the latest last access time for key %s", max, key);
                  }
                  long longMax = ((Long) max);
                  if (longMax == -1) {
                     // If it was -1 that means it has been written to, so in this case just assume it expired, but
                     // was overwritten by a concurrent write
                     return Boolean.TRUE;
                  } else {
                     // If it wasn't -1 it has to be > 0 so send that update
                     UpdateLastAccessCommand ulac = cf.buildUpdateLastAccessCommand(key, command.getSegment(), longMax);
                     ulac.setTopologyId(cacheTopology.getTopologyId());
                     CompletionStage<?> updateState = rpcManager.invokeCommand(info.readOwners(), ulac,
                           VoidResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
                     ulac.inject(dataContainer);
                     // We update locally as well
                     ulac.invokeAsync();
                     return asyncValue(updateState).thenApply(rCtx, rCommand, (rCtx2, rCommand2, ignore) -> Boolean.FALSE);
                  }
               } else {
                  if (trace) {
                     log.tracef("No node has a non expired max idle time for key %s, proceeding to remove entry", key);
                  }
                  RemoveExpiredCommand realRemoveCommand;
                  if (!ctx.isOriginLocal()) {
                     // Have to build a new command since the command id points to the originating node - causes
                     // issues with triangle since it needs to know the originating node to respond to
                     realRemoveCommand = cf.buildRemoveExpiredCommand(key, command.getValue(), segment,
                           command.getFlagsBitSet());
                     realRemoveCommand.setTopologyId(cacheTopology.getTopologyId());
                  } else {
                     realRemoveCommand = command;
                  }

                  return makeStage(invokeRemoveExpiredCommand(ctx, realRemoveCommand, info)).thenApply(rCtx, rCommand,
                        (rCtx2, rCommand2, ignore) -> Boolean.TRUE);
               }
            });
         } else if (ctx.isOriginLocal()) {
            // Primary has to handle max idle removal
            return invokeRemotely(ctx, command, info.primary());
         } else {
            return invokeNext(ctx, command);
         }
      }
   }

   protected Object invokeRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command, DistributionInfo distributionInfo)
         throws Throwable {
      assert distributionInfo.isPrimary();
      return visitRemoveCommand(ctx, command);
   }
}
