package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
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
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
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
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CommandAckCollector;
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
 * @author Dan Berindei <dan@infinispan.org>
 */
public abstract class BaseDistributionInterceptor extends ClusteringInterceptor {
   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Object LOST_PLACEHOLDER = new Object();

   @Inject protected DistributionManager dm;
   @Inject protected RemoteValueRetrievedListener rvrl;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected ClusteringDependentLogic cdl;
   @Inject protected InvocationManager invocationManager;
   @Inject protected TimeService timeService;

   protected boolean isL1Enabled;
   protected boolean isReplicated;

   private final ReadOnlyManyHelper readOnlyManyHelper = new ReadOnlyManyHelper();
   private final InvocationSuccessFunction updateBackupsAndReturn = this::updateBackupsAndReturn;
   protected final RetrieveRemoteOnBackupHandler retrieveRemoteAndMaybeApply = this::retrieveRemoteAndMaybeApply;
   private final InvocationSuccessFunction handleBackupRetrieval = this::handleBackupRetrieval;

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
      Address primaryOwner = dm.getCacheTopology().getDistribution(command.getGroupName()).primary();
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

   protected CompletionStage<Void> remoteGet(InvocationContext ctx, TopologyAffectedCommand command,
                                             Object key, boolean isWrite, long flagsBitSet) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      int currentTopologyId = cacheTopology.getTopologyId();

      DistributionInfo info = cacheTopology.getDistribution(key);
      if (info.isReadOwner()) {
         if (trace) {
            log.tracef("Key %s became local after wrapping, retrying command. Command topology is %d, current topology is %d",
                  key, command, currentTopologyId);
         }
         // The topology has changed between EWI and BDI, let's retry
         if (command.getTopologyId() == currentTopologyId) {
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

      if (isLocalModeForced(command)) {
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
         return invokeNext(ctx, command);
      }

      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo distributionInfo = cacheTopology.getDistribution(key);

      if (distributionInfo.isPrimary()) {
         if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
            checkInvocationRecord(ctx, command, command.getKey());
         }
         return invokeNextThenApply(ctx, command, updateBackupsAndReturn);
      } else if (ctx.isOriginLocal()) {
         return invokeRemotely(ctx, command, distributionInfo);
      } else {
         CompletionStage<?> remoteRead = handleBackupWrite(ctx, command, command.getKey(), k -> distributionInfo, retrieveRemoteAndMaybeApply);
         if (remoteRead == null) {
            if (command.isCompleted(key)) {
               return null;
            } else {
               return invokeNext(ctx, command);
            }
         } else {
            return asyncValue(remoteRead).thenApply(ctx, command, handleBackupRetrieval);
         }
      }
   }

   protected interface RetrieveRemoteOnBackupHandler {
      CompletionStage<?> handle(InvocationContext ctx, WriteCommand command, Object key, DistributionInfo distributionInfo);
   }

   protected CompletionStage<?> handleBackupWrite(InvocationContext ctx, WriteCommand command, Object key, Function<Object, DistributionInfo> distributionInfo,
                                      RetrieveRemoteOnBackupHandler retrieveRemote) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry == null) {
         if (trace) {
            log.trace("Not a read owner, will fetch previous value");
         }
         return retrieveRemote.handle(ctx, command, key, distributionInfo.apply(key));
      }
      // If the command is not retried the last invocation id is not set
      // TODO? verify that we can skip the check
      if (!command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         return null;
      }
      CommandInvocationId lastInvocationId = command.getLastInvocationId(key);
      Metadata metadata = entry.getMetadata();
      CommandInvocationId entryInvocationId = null;
      InvocationRecord commandInvocationRecord = null;
      if (metadata != null) {
         commandInvocationRecord = metadata.invocation(command.getCommandInvocationId());
         if (commandInvocationRecord != null) {
            entryInvocationId = commandInvocationRecord.lastId();
         } else {
            InvocationRecord entryLastRecord = metadata.lastInvocation();
            if (entryLastRecord != null) {
               entryInvocationId = entryLastRecord.getId();
            }
         }
      }
      if (commandInvocationRecord != null) {
         if (Objects.equals(lastInvocationId, entryInvocationId)) {
            if (trace) {
               log.tracef("Not executing %s as it was already executed and previous id %s matches", command, lastInvocationId);
            }
            commandInvocationRecord.touch(timeService.wallClockTime());
            command.setCompleted(key, true);
            return null;
         } else {
            if (trace) {
               log.tracef("Fetching full history as previous id in entry (%s) does not match command (%s)", entryInvocationId, lastInvocationId);
            }
            commandInvocationRecord.touch(timeService.wallClockTime());
            // Note that it is not guaranteed that the history will contain the command execution
            return retrieveRemote.handle(ctx, command, key, distributionInfo.apply(key));
         }
      } else if (Objects.equals(lastInvocationId, entryInvocationId)) {
         if (trace) {
            log.trace("Applying command as it was not executed yet and the last invocations match");
         }
         return null;
      } else {
         if (trace) {
            log.tracef("Fetching full history as current id in entry (%s) does not match command (%s)", entryInvocationId, lastInvocationId);
         }
         return retrieveRemote.handle(ctx, command, key, distributionInfo.apply(key));
      }
   }

   private CompletionStage<?> retrieveRemoteAndMaybeApply(InvocationContext ctx, WriteCommand command, Object key, DistributionInfo distributionInfo) {
      ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(key, FlagBitSets.WITH_INVOCATION_RECORDS);
      clusteredGetCommand.setTopologyId(command.getTopologyId());
      // TODO: Unsure response should throw OutdatedTopologyException
      return rpcManager.invokeCommand(distributionInfo.primary(), clusteredGetCommand, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
            .thenAccept(rsp -> {
               Object responseValue = rsp.getResponseValue();

               CacheEntry<?, ?> cacheEntry;
               if (responseValue == null) {
                  cacheEntry = NullCacheEntry.getInstance();
               } else {
                  cacheEntry = ((InternalCacheValue) responseValue).toInternalCacheEntry(key);
               }
               wrapRemoteEntry(ctx, key, cacheEntry, true);
            });
   }

   private Object handleBackupRetrieval(InvocationContext rCtx, VisitableCommand rCommand, Object nil) {
      DataWriteCommand writeCommand = (DataWriteCommand) rCommand;
      handleBackupWrite(rCtx, writeCommand, writeCommand.getKey(), null, BaseDistributionInterceptor::unexpectedRetrieval);
      if (writeCommand.isCompleted(writeCommand.getKey())) {
         return null;
      } else {
         return invokeNext(rCtx, writeCommand);
      }
   }

   protected static CompletableFuture<?> unexpectedRetrieval(InvocationContext ctx, WriteCommand command, Object key, DistributionInfo distributionInfo) {
      throw new IllegalStateException();
   }

   private Object invokeRemotely(InvocationContext ctx, DataWriteCommand command, DistributionInfo distributionInfo) {
      if (trace) log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", distributionInfo.primary());
      boolean isSyncForwarding = isSynchronous(command) || command.isReturnValueExpected();

      if (!isSyncForwarding) {
         rpcManager.sendTo(distributionInfo.primary(), command, DeliverOrder.PER_SENDER);
         return null;
      }
      CompletionStage<ValidResponse> remoteInvocation = rpcManager.invokeCommand(distributionInfo.primary(), command,
            SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions());
      return asyncValue(remoteInvocation).andHandle(ctx, command, (rCtx, rCommand, rv, t) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         CompletableFutures.rethrowException(t);

         Response response = ((Response) rv);
         if (!response.isSuccessful()) {
            dataWriteCommand.fail();
            // FIXME A response cannot be successful and not valid
         } else if (!(response instanceof ValidResponse)) {
            throw unexpected(response);
         }
         invocationManager.notifyCompleted(dataWriteCommand.getCommandInvocationId(), command.getKey(), distributionInfo.segmentId());
         // We expect only successful/unsuccessful responses, not unsure
         return ((ValidResponse) response).getResponseValue();
      });
   }

   private Object updateBackupsAndReturn(InvocationContext ctx, VisitableCommand visitableCommand, Object localResult) {
      DataWriteCommand command = (DataWriteCommand) visitableCommand;
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo distributionInfo = cacheTopology.getDistribution(command.getKey());
      if (!command.isSuccessful()) {
         if (trace) log.trace("Skipping the replication as the command did not succeed on primary owner.");
         return localResult;
      } else if (distributionInfo.writeBackups().isEmpty()) {
         if (trace) log.trace("Skipping the replication, no backups");
         return localResult;
      }

      if (!isSynchronous(command)) {
         if (isReplicated) {
            rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
         } else {
            rpcManager.sendToMany(distributionInfo.writeBackups(), command, DeliverOrder.PER_SENDER);
         }
         return localResult;
      }
      // TODO: set flags so that backup does not try to return any value
      // but local interceptors can consume it - SKIP_REMOTE_LOOKUP?
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(isReplicated, distributionInfo.writeBackups().size());
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<Map<Address, Response>> remoteInvocation = isReplicated ?
            rpcManager.invokeCommandOnAll(command, collector, rpcOptions) :
            rpcManager.invokeCommand(distributionInfo.writeBackups(), command, collector, rpcOptions);
      return asyncValue(remoteInvocation.handle((responses, t) -> {
         CompletableFutures.rethrowException(t instanceof RemoteException ? t.getCause() : t);
         if (ctx.isOriginLocal() && isSynchronous(command)) {
            invocationManager.notifyCompleted(command.getCommandInvocationId(), command.getKey(), distributionInfo.segmentId());
         }
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

   protected void handleRemotelyRetrievedKeys(InvocationContext ctx, List<?> remoteKeys) {
   }

   protected void handleClusteredGetAllResponse(InvocationContext ctx, List<?> keys, SuccessfulResponse response, ClusteredGetAllFuture allFuture, boolean isWrite) {
      Object responseValue = response.getResponseValue();
      if (!(responseValue instanceof InternalCacheValue[])) {
         allFuture.completeExceptionally(new IllegalStateException("Unexpected response value: " + responseValue));
         return;
      }
      InternalCacheValue[] values = (InternalCacheValue[]) responseValue;
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
            wrapRemoteEntry(ctx, key, entry, isWrite);
         }
         handleRemotelyRetrievedKeys(ctx, keys);
         if (--allFuture.counter == 0) {
            allFuture.complete(null);
         }
      }
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
         handleClusteredGetAllResponse(ctx, keys, response, allFuture, false);
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

   /**
    * This method should be invoked only on primary owner.
    * If the command was already executed sets {@link WriteCommand#setCompleted(Object, boolean)} to do a 'dry-run'
    * on the value and rewinds the cache entry to the value before command execution.
    */
   protected void checkInvocationRecord(InvocationContext context, WriteCommand command, Object key) {
      CacheEntry cacheEntry = context.lookupEntry(key);
      if (cacheEntry == null) {
         DistributionInfo distribution = dm.getCacheTopology().getDistribution(key);
         throw new IllegalStateException("No entry for key " + key + ", readOwners="
               + distribution.readOwners() + ", writeOwners=" + distribution.writeOwners());
      }
      Metadata metadata = cacheEntry.getMetadata();
      if (metadata == null) {
         if (trace) log.trace("No metadata, will proceed");
         return;
      }
      InvocationRecord currentLastInvocation = metadata.lastInvocation();
      InvocationRecord invocationRecord = metadata.invocation(command.getCommandInvocationId());
      // having invocation record implies that the command was successful
      if (invocationRecord == null) {
         if (currentLastInvocation != null) {
            // last invocation id is irrelevant if the command was already invoked, because
            // we won't send the previous value from owner
            command.setLastInvocationId(key, currentLastInvocation.getId());
         }
         if (trace) {
            log.tracef("No invocation record, current last invocation %s", currentLastInvocation);
         }
         return;
      }
      CommandInvocationId previousId = invocationRecord.lastId();
      if (previousId != null) {
         command.setLastInvocationId(key, previousId);
      }
      invocationRecord.touch(timeService.wallClockTime());
      command.setCompleted(key, true);
      cacheEntry.setValue(invocationRecord.previousValue);
      cacheEntry.setMetadata(invocationRecord.previousMetadata);
      if (invocationRecord.previousValue == null) {
         cacheEntry.setCreated(true);
      }
      if (trace) {
         log.tracef("Command %s was already executed on key %s, rewinding to %s.",
               command, key, invocationRecord.previousValue);
      }
   }

   // this should be executed only on the primary owner
   protected void checkInvocationRecords(InvocationContext ctx, WriteCommand command) {
      for (Object key : command.getAffectedKeys()) {
         checkInvocationRecord(ctx, command, key);
      }
   }

   // we're assuming that this function is ran on primary owner of given segments
   protected Map<Address, Set<Integer>> backupOwnersOfSegments(ConsistentHash ch) {
      Map<Address, Set<Integer>> map = new HashMap<>(ch.getMembers().size());
      Set<Integer> segments = ch.getPrimarySegmentsForOwner(rpcManager.getAddress());
      if (ch.isReplicated()) {
         for (Address member : ch.getMembers()) {
            map.put(member, segments);
         }
         map.remove(rpcManager.getAddress());
      } else {
         for (Integer segment : segments) {
            Iterator<Address> iterator = ch.locateOwnersForSegment(segment).iterator();
            if (iterator.hasNext()) iterator.next(); // drop primary owner
            while (iterator.hasNext()) {
               Address owner = iterator.next();
               map.computeIfAbsent(owner, o -> new HashSet<>()).add(segment);
            }
         }
      }
      return map;
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
            asyncInvokeNext(ctx, command, remoteGet(ctx, command, command.getKey(), false, command.getFlagsBitSet())) :
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

   protected LocalizedCacheTopology checkTopologyId(TopologyAffectedCommand command) {
      LocalizedCacheTopology cacheTopology = dm.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (command instanceof FlagAffectedCommand && ((((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL)))) {
         log.tracef("Skipping topology check for command %s", command);
         return cacheTopology;
      }
      // TotalOrderStateTransferInterceptor does not set topologyId for write commands
      if (cmdTopology >= 0 && currentTopologyId != cmdTopology) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
            cmdTopology + ", got " + currentTopologyId);
      }
      if (trace) {
         log.tracef("Current topology %d, command topology %d", currentTopologyId, cmdTopology);
      }
      if (cmdTopology >= 0 && currentTopologyId != cmdTopology) {
         throw OutdatedTopologyException.INSTANCE;
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
      ReadOnlyManyCommand copyForRemote(C command, List<Object> keys, InvocationContext ctx);
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
      protected final Collection<Object>[] keysBySegment;
      private final LocalizedCacheTopology cacheTopology;
      private final WriteManyCommandHelper<?, Container, Item> helper;

      protected PrimaryOwnerClassifier(LocalizedCacheTopology cacheTopology, int entryCount, WriteManyCommandHelper<?, Container, Item> helper) {
         this.cacheTopology = cacheTopology;
         this.keysBySegment =  new Collection[cacheTopology.numSegments()];
         int memberSize = cacheTopology.getMembers().size();
         this.primaries = new HashMap<>(memberSize);
         this.helper = helper;
      }

      public Collection<?>[] keysBySegment() {
         return keysBySegment;
      }

      public void add(Item item) {
         Object key = helper.item2key(item);
         int segment = cacheTopology.getSegment(key);
         DistributionInfo distributionInfo = cacheTopology.getDistributionForSegment(segment);
         add(key, item, distributionInfo);
      }

      protected void add(Object key, Item item, DistributionInfo distributionInfo) {
         final Address primaryOwner = distributionInfo.primary();
         Collection<Object> keysInSegment = keysBySegment[distributionInfo.segmentId()];
         if (keysInSegment == null) {
            keysBySegment[distributionInfo.segmentId()] = keysInSegment = new ArrayList<>();
         }
         keysInSegment.add(key);
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
   }
}
