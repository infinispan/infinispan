package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
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
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ArrayCollector;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.CacheTopologyUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private static final Object LOST_PLACEHOLDER = new Object();

   @Inject protected RemoteValueRetrievedListener rvrl;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected TimeService timeService;
   @Inject protected InternalExpirationManager<Object, Object> expirationManager;

   protected boolean isL1Enabled;
   protected boolean isReplicated;

   private final ReadOnlyManyHelper readOnlyManyHelper = new ReadOnlyManyHelper();
   private final InvocationSuccessFunction<AbstractDataWriteCommand> primaryReturnHandler = this::primaryReturnHandler;

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

   /**
    * Fetch a key from its remote owners and store it in the context.
    *
    * <b>Not thread-safe</b>. The invocation context should not be accessed concurrently from multiple threads,
    * so this method should only be used for single-key commands.
    */
   protected <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletionStage<Void> remoteGetSingleKey(
         InvocationContext ctx, C command, Object key, boolean isWrite) {
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      int topologyId = cacheTopology.getTopologyId();

      DistributionInfo info = retrieveDistributionInfo(cacheTopology, command, key);
      if (info.isReadOwner()) {
         if (log.isTraceEnabled()) {
            log.tracef("Key %s became local after wrapping, retrying command. Command topology is %d, current topology is %d",
                  key, command.getTopologyId(), topologyId);
         }
         // The topology has changed between EWI and BDI, let's retry
         if (command.getTopologyId() == topologyId) {
            throw new IllegalStateException();
         }
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Perform remote get for key %s. currentTopologyId=%s, owners=%s",
            key, topologyId, info.readOwners());
      }

      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, info.segmentId(), command.getFlagsBitSet());
      getCommand.setTopologyId(topologyId);
      getCommand.setWrite(isWrite);

      return rpcManager.invokeCommandStaggered(info.readOwners(), getCommand, new RemoteGetSingleKeyCollector(),
                                               rpcManager.getSyncRpcOptions())
                       .thenAccept(response -> {
                          InternalCacheValue<?> responseValue = response.getResponseObject();
                          if (responseValue == null) {
                             if (rvrl != null) {
                                rvrl.remoteValueNotFound(key);
                             }
                             wrapRemoteEntry(ctx, key, NullCacheEntry.getInstance(), isWrite);
                             return;
                          }
                          InternalCacheEntry<?, ?> ice = responseValue.toInternalCacheEntry(key);
                          if (rvrl != null) {
                             rvrl.remoteValueFound(ice);
                          }
                          wrapRemoteEntry(ctx, key, ice, isWrite);
                       });
   }

   protected void wrapRemoteEntry(InvocationContext ctx, Object key, CacheEntry ice, boolean isWrite) {
      entryFactory.wrapExternalEntry(ctx, key, ice, true, isWrite);
   }

   protected final Object handleNonTxWriteCommand(InvocationContext ctx, AbstractDataWriteCommand command) {
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);

      if (isLocalModeForced(command)) {
         if (entry == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      DistributionInfo info = cacheTopology.getSegmentDistribution(SegmentSpecificCommand.extractSegment(command, key,
            keyPartitioner));

      if (isReplicated && command.hasAnyFlag(FlagBitSets.BACKUP_WRITE) && !info.isWriteOwner()) {
         // Replicated caches receive broadcast commands even when they are not owners (e.g. zero capacity nodes)
         // The originator will ignore the UnsuccessfulResponse
         command.fail();
         return null;
      }

      if (entry == null) {
         boolean load = shouldLoad(ctx, command, info);
         if (info.isPrimary()) {
            throw new IllegalStateException("Primary owner in writeCH should always be an owner in readCH as well.");
         } else if (ctx.isOriginLocal()) {
            return invokeRemotely(ctx, command, info.primary());
         } else {
            if (load) {
               CompletionStage<?> remoteGet = remoteGetSingleKey(ctx, command, command.getKey(), true);
               return asyncInvokeNext(ctx, command, remoteGet);
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

   protected Object primaryReturnHandler(InvocationContext ctx, AbstractDataWriteCommand command, Object localResult) {
      if (!command.isSuccessful()) {
         if (log.isTraceEnabled()) log.tracef("Skipping the replication of the conditional command as it did not succeed on primary owner (%s).", command);
         return localResult;
      }
      if (!command.shouldReplicate(ctx, false)) {
         if (log.isTraceEnabled()) log.tracef("Skipping the replication of the command as it does not need to be (%s).", command);
         return localResult;
      }
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      int segment = SegmentSpecificCommand.extractSegment(command, command.getKey(), keyPartitioner);
      DistributionInfo distributionInfo = cacheTopology.getSegmentDistribution(segment);
      Collection<Address> owners = distributionInfo.writeOwners();
      if (owners.size() == 1) {
         // There are no backups, skip the replication part.
         return localResult;
      }
      // Cache the matcher and reset it if we get OOTE (or any other exception) from backup
      ValueMatcher originalMatcher = command.getValueMatcher();
      // Ignore the previous value on the backup owners
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      boolean hadIgnoreReturnValues = command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES);
      command.addFlags(FlagBitSets.IGNORE_RETURN_VALUES);
      if (!isSynchronous(command)) {
         if (isReplicated) {
            rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
         } else {
            rpcManager.sendToMany(owners, command, DeliverOrder.PER_SENDER);
         }
         // Switch to the retry policy, in case the primary owner changes before we commit locally
         command.setValueMatcher(originalMatcher.matcherForRetry());
         // Need to unset flag in case if command is retried or if was not a locally invoked command to have a response generated
         if (!hadIgnoreReturnValues) command.setFlagsBitSet(command.getFlagsBitSet() & ~FlagBitSets.IGNORE_RETURN_VALUES);
         return localResult;
      }
      VoidResponseCollector collector = VoidResponseCollector.ignoreLeavers();
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      // Mark the command as a backup write so it can skip some checks
      command.addFlags(FlagBitSets.BACKUP_WRITE);
      CompletionStage<Void> remoteInvocation = isReplicated ?
            rpcManager.invokeCommandOnAll(command, collector, rpcOptions) :
            rpcManager.invokeCommand(owners, command, collector, rpcOptions);
      return asyncValue(remoteInvocation.handle((ignored, t) -> {
         // Unset the backup write bit as the command will be retried
         command.setFlagsBitSet(command.getFlagsBitSet() & ~FlagBitSets.BACKUP_WRITE);
         // Need to unset flag in case if command is retried or if was not a locally invoked command to have a response generated
         if (!hadIgnoreReturnValues) command.setFlagsBitSet(command.getFlagsBitSet() & ~FlagBitSets.IGNORE_RETURN_VALUES);
         // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
         CompletableFutures.rethrowExceptionIfPresent(t);
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
      CompletionStage<Void> remoteGetFuture = remoteGetMany(ctx, command, command.getKeys());
      return asyncInvokeNext(ctx, command, remoteGetFuture);
   }

   protected <C extends FlagAffectedCommand & TopologyAffectedCommand>
   CompletionStage<Void> remoteGetMany(InvocationContext ctx, C command, Collection<?> keys) {
      return doRemoteGetMany(ctx, command, keys, null, false);
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand>
   CompletionStage<Void> doRemoteGetMany(InvocationContext ctx, C command, Collection<?> keys,
                                         Map<Object, Collection<Address>> unsureOwners, boolean hasSuspectedOwner) {
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      Map<Address, List<Object>> requestedKeys = getKeysByOwner(ctx, keys, cacheTopology, null, unsureOwners);
      if (requestedKeys.isEmpty()) {
         for (Object key : keys) {
            if (ctx.lookupEntry(key) == null) {
               // We got an UnsureResponse or CacheNotFoundResponse from all the owners, retry
               if (hasSuspectedOwner) {
                  // After all the owners are lost, we must wait for a new topology in case the key is still available
                  throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
               } else {
                  // If we got only UnsureResponses we can retry without waiting, see RETRY_SAME_TOPOLOGY javadoc
                  throw OutdatedTopologyException.RETRY_SAME_TOPOLOGY;
               }
            }
         }
         return CompletableFutures.completedNull();
      }

      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      ClusteredReadCommandGenerator commandGenerator =
         new ClusteredReadCommandGenerator(requestedKeys, command.getFlagsBitSet(), command.getTopologyId(), gtx);
      RemoteGetManyKeyCollector collector = new RemoteGetManyKeyCollector(requestedKeys, ctx, command, unsureOwners,
                                                                          hasSuspectedOwner);
      // We cannot retry in the collector, because it can't return a CompletionStage
      return rpcManager.invokeCommands(requestedKeys.keySet(), commandGenerator, collector,
                                       rpcManager.getSyncRpcOptions())
                       .thenCompose(unsureOwners1 -> {
                          Collection<?> keys1 = unsureOwners1 != null ? unsureOwners1.keySet() : Collections.emptyList();
                          return doRemoteGetMany(ctx, command, keys1, unsureOwners1, collector.hasSuspectedOwner());
                       });
   }

   protected void handleRemotelyRetrievedKeys(InvocationContext ctx, WriteCommand appliedCommand, List<?> remoteKeys) {
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

      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
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

      CompletionStage<Void> requiredKeysFuture = helper.fetchRequiredKeys(cacheTopology, requestedKeys, availableKeys,
                                                                          ctx, command);
      if (requiredKeysFuture == null) {
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
         InvocationContext ctx, C command, Collection<?> keys, InvocationSuccessFunction<C> remoteReturnHandler) {
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
      private final ReadManyCommandHelper<C> helper;

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
               allFuture.completeExceptionally(new IllegalStateException("Unexpected response " + response));
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
            requestedKeys = getKeysByOwner(ctx, keys, CacheTopologyUtil.checkTopology(command, getCacheTopology()), null, contactedNodes);
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
                  throw new IllegalStateException("Entry should already be wrapped on read owners!");
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
      if (ctx.lookupEntry(command.getKey()) != null) {
         return invokeNext(ctx, command);
      }

      if (!ctx.isOriginLocal())
         return UnsureResponse.INSTANCE;

      if (!readNeedsRemoteValue(command))
         return null;

      return asyncInvokeNext(ctx, command, remoteGetSingleKey(ctx, command, command.getKey(), false));
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
         LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
         Collection<Address> owners = cacheTopology.getDistribution(key).readOwners();
         if (log.isTraceEnabled())
            log.tracef("Doing a remote get for key %s in topology %d to %s", key, cacheTopology.getTopologyId(), owners);

         ReadOnlyKeyCommand remoteCommand = remoteReadOnlyCommand(ctx, command);
         // make sure that the command topology is set to the value according which we route it
         remoteCommand.setTopologyId(cacheTopology.getTopologyId());

         CompletionStage<SuccessfulResponse> rpc =
            rpcManager.invokeCommandStaggered(owners, remoteCommand, new RemoteGetSingleKeyCollector(),
                                              rpcManager.getSyncRpcOptions());
         return asyncValue(rpc).thenApply(ctx, command, (rCtx, rCommand, response) ->
               unwrapFunctionalResultOnOrigin(rCtx, rCommand.getKey(), ((SuccessfulResponse) response).getResponseObject()));
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
      if (log.isTraceEnabled()) getLog().tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
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
      return asyncValue(remoteInvocation).andHandle(ctx, command, (rCtx, dataWriteCommand, rv, t) -> {
         dataWriteCommand.setValueMatcher(dataWriteCommand.getValueMatcher().matcherForRetry());
         CompletableFutures.rethrowExceptionIfPresent(t);

         Response response = ((Response) rv);
         if (!response.isSuccessful()) {
            dataWriteCommand.fail();
            // FIXME A response cannot be successful and not valid
         } else if (!(response instanceof ValidResponse)) {
            throw unexpected(primaryOwner, response);
         }
         // We expect only successful/unsuccessful responses, not unsure
         return ((ValidResponse) response).getResponseObject();
      });
   }

   /**
    * @return {@code true} if the value is not available on the local node and a read command is allowed to
    * fetch it from a remote node. Does not check if the value is already in the context.
    */
   protected boolean readNeedsRemoteValue(FlagAffectedCommand command) {
      return !command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP);
   }

   protected interface ReadManyCommandHelper<C extends VisitableCommand> extends InvocationSuccessFunction<C> {
      Collection<?> keys(C command);
      C copyForLocal(C command, List<Object> keys);
      ReadOnlyManyCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx);
      void applyLocalResult(MergingCompletableFuture allFuture, Object rv);
      Object transformResult(Object[] results);

      CompletionStage<Void> fetchRequiredKeys(LocalizedCacheTopology cacheTopology,
                                              Map<Address, List<Object>> requestedKeys, List<Object> availableKeys,
                                              InvocationContext ctx, C command);
   }

   protected class ReadOnlyManyHelper implements ReadManyCommandHelper<ReadOnlyManyCommand> {
      @Override
      public Object apply(InvocationContext rCtx, ReadOnlyManyCommand rCommand, Object rv) throws Throwable {
         return wrapFunctionalManyResultOnNonOrigin(rCtx, rCommand.getKeys(), ((Stream) rv).toArray());
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
      public CompletionStage<Void> fetchRequiredKeys(LocalizedCacheTopology cacheTopology,
                                                     Map<Address, List<Object>> requestedKeys,
                                                     List<Object> availableKeys, InvocationContext ctx,
                                                     ReadOnlyManyCommand command) {
         return null;
      }
   }

   private class ClusteredReadCommandGenerator implements Function<Address, ReplicableCommand> {
      private final Map<Address, List<Object>> requestedKeys;
      private final long flags;
      private final int topologyId;
      private final GlobalTransaction gtx;

      public ClusteredReadCommandGenerator(Map<Address, List<Object>> requestedKeys, long flags, int topologyId,
                                           GlobalTransaction gtx) {
         this.requestedKeys = requestedKeys;
         this.flags = flags;
         this.topologyId = topologyId;
         this.gtx = gtx;
      }

      @Override
      public ReplicableCommand apply(Address target) {
         List<Object> targetKeys = requestedKeys.get(target);
         assert !targetKeys.isEmpty();
         ClusteredGetAllCommand<?,?> getCommand = cf.buildClusteredGetAllCommand(targetKeys, flags, gtx);
         getCommand.setTopologyId(topologyId);
         return getCommand;
      }
   }

   /**
    * Response collector for multi-key multi-target remote read commands.
    *
    * <p>Wrap in the context all received values, unless receiving an exception or unexpected response.
    * Throw an exception immediately if a response is exceptional or unexpected.
    * After processing all responses, if any of them were either {@link UnsureResponse} or
    * {@link CacheNotFoundResponse}, throw an {@link OutdatedTopologyException}.</p>
    */
   private class RemoteGetManyKeyCollector implements ResponseCollector<Map<Object, Collection<Address>>> {
      private final Map<Address, List<Object>> requestedKeys;
      private final InvocationContext ctx;
      private final ReplicableCommand command;

      private Map<Object, Collection<Address>> unsureOwners;
      private boolean hasSuspectedOwner;

      public RemoteGetManyKeyCollector(Map<Address, List<Object>> requestedKeys, InvocationContext ctx,
                                       ReplicableCommand command, Map<Object, Collection<Address>> unsureOwners,
                                       boolean hasSuspectedOwner) {
         this.requestedKeys = requestedKeys;
         this.ctx = ctx;
         this.command = command;
         this.unsureOwners = unsureOwners;
         this.hasSuspectedOwner = hasSuspectedOwner;
      }

      @Override
      public Map<Object, Collection<Address>> addResponse(Address sender, Response response) {
         if (!(response instanceof SuccessfulResponse)) {
            if (response instanceof CacheNotFoundResponse) {
               hasSuspectedOwner = true;
               addUnsureOwner(sender);
               return null;
            } else if (response instanceof UnsureResponse) {
               addUnsureOwner(sender);
               return null;
            } else {
               if (response instanceof ExceptionResponse) {
                  throw CompletableFutures.asCompletionException(((ExceptionResponse) response).getException());
               } else {
                  throw unexpected(sender, response);
               }
            }
         }

         SuccessfulResponse successfulResponse = (SuccessfulResponse) response;
         InternalCacheValue<?>[] values = successfulResponse.getResponseArray(new InternalCacheValue[0]);
         if (values == null)
            throw CompletableFutures.asCompletionException(new IllegalStateException("null response value"));

         List<Object> senderKeys = requestedKeys.get(sender);
         for (int i = 0; i < senderKeys.size(); ++i) {
            Object key = senderKeys.get(i);
            InternalCacheValue<?> value = values[i];
            CacheEntry<?, ?> entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
            wrapRemoteEntry(ctx, key, entry, command instanceof WriteCommand);
         }
         // TODO Dan: handleRemotelyRetrievedKeys could call wrapRemoteEntry itself after transforming the entries
         handleRemotelyRetrievedKeys(ctx, command instanceof WriteCommand ? (WriteCommand) command : null, senderKeys);
         return null;
      }

      public void addUnsureOwner(Address sender) {
         if (unsureOwners == null) {
            unsureOwners = new HashMap<>();
         }
         List<Object> senderKeys = requestedKeys.get(sender);
         for (Object key : senderKeys) {
            Collection<Address> keyUnsureOwners = unsureOwners.get(key);
            if (keyUnsureOwners == null) {
               keyUnsureOwners = new ArrayList<>();
               unsureOwners.put(key, keyUnsureOwners);
            }
            keyUnsureOwners.add(sender);
         }
      }

      @Override
      public Map<Object, Collection<Address>> finish() {
         return unsureOwners;
      }

      public boolean hasSuspectedOwner() {
         return hasSuspectedOwner;
      }
   }
}
