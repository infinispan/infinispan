package org.infinispan.interceptors.distribution;

import static org.infinispan.commands.VisitableCommand.LoadType.OWNER;
import static org.infinispan.commands.VisitableCommand.LoadType.PRIMARY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.BackupPutMapRpcCommand;
import org.infinispan.commands.write.BackupWriteRpcCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Non-transactional interceptor used by distributed caches that supports concurrent writes.
 * <p>
 * It is implemented based on the Triangle algorithm.
 * <p>
 * The {@link GetKeyValueCommand} reads the value locally if it is available (the node is an owner or the value is
 * stored in L1). If it isn't available, a remote request is made. The {@link DataWriteCommand} is performed as follow:
 * <ul> <li>The command if forwarded to the primary owner of the key.</li> <li>The primary owner locks the key and
 * executes the operation; sends the {@link BackupWriteRpcCommand} to the backup owners; releases the lock; sends the
 * {@link SuccessfulResponse} or {@link UnsuccessfulResponse} back to the originator.</li>
 * <li>The backup owner applies the update and sends a {@link
 * BackupAckCommand} back to the originator.</li> <li>The originator collects the ack from all the owners and
 * returns.</li> </ul> The {@link PutMapCommand} is performed in a similar way: <ul> <li>The subset of the map is split
 * by primary owner.</li> <li>The primary owner locks the key and executes the command; splits the keys by backup owner
 * and send them; and replies to the originator.</li> <li>The backup owner applies the update and sends back the {@link
 * BackupMultiKeyAckCommand} to the originator.</li> <li>The originator collects all the acknowledges from all owners
 * and returns.</li> </ul> The acknowledges management is done by the {@link CommandAckCollector}.
 * <p>
 * If a topology changes while a command is executed, an {@link OutdatedTopologyException} is thrown. The {@link
 * StateTransferInterceptor} will catch it and retries the command.
 * <p>
 * TODO: finish the wiki page and add a link to it!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleDistributionInterceptor extends NonTxDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TriangleDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   @Inject private CommandAckCollector commandAckCollector;
   @Inject private CommandsFactory commandsFactory;
   @Inject private TriangleOrderManager triangleOrderManager;
   private Address localAddress;

   private static Map<Object, Object> mergeMaps(ValidResponse response, Map<Object, Object> resultMap) {
      //noinspection unchecked
      Map<Object, Object> remoteMap = (Map<Object, Object>) response.getResponseValue();
      return InfinispanCollections.mergeMaps(resultMap, remoteMap);
   }

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command);
      } else {
         return handleRemotePutMapCommand(ctx, command);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   private Object handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      final VisitableCommand.LoadType loadType = command.loadType();

      if (command.isForwarded()) {
         //backup & remote || no backups
         return asyncInvokeNext(ctx, command,
               checkRemoteGetIfNeeded(ctx, command, command.getMap().keySet(), cacheTopology, loadType == OWNER));
      }
      //primary, we need to send the command to the backups ordered!
      sendToBackups(command, command.getMap(), cacheTopology);
      return asyncInvokeNext(ctx, command,
            checkRemoteGetIfNeeded(ctx, command, command.getMap().keySet(), cacheTopology, loadType == OWNER));
   }

   private void sendToBackups(PutMapCommand command, Map<Object, Object> entries, LocalizedCacheTopology cacheTopology) {
      BackupOwnerClassifier filter = new BackupOwnerClassifier(cacheTopology, entries.size());
      entries.entrySet().forEach(filter::add);
      int topologyId = command.getTopologyId();
      for (Map.Entry<Integer, Map<Object, Object>> entry : filter.perSegmentKeyValue.entrySet()) {
         int segmentId = entry.getKey();
         Collection<Address> backups = cacheTopology.getDistributionForSegment(segmentId).writeBackups();
         if (backups.isEmpty()) {
            // Only the primary owner. Other segments may have more than one owner, e.g. during rebalance.
            continue;
         }
         Map<Object, Object> map = entry.getValue();
         long sequence = triangleOrderManager.next(segmentId, topologyId);
         BackupPutMapRpcCommand backupPutMapRpcCommand = commandsFactory.buildBackupPutMapRpcCommand(command);
         backupPutMapRpcCommand.setMap(map);
         backupPutMapRpcCommand.setSequence(sequence);
         if (trace) {
            log.tracef("Command %s got sequence %s for segment %s", command.getCommandInvocationId(), segmentId,
                  sequence);
         }
         rpcManager.sendToMany(backups, backupPutMapRpcCommand, DeliverOrder.NONE);
      }
   }

   private Object handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      //local command. we need to split by primary owner to send the command to them
      final LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      final PrimaryOwnerClassifier filter = new PrimaryOwnerClassifier(cacheTopology, command.getMap().size());
      final boolean sync = isSynchronous(command);
      final VisitableCommand.LoadType loadType = command.loadType();

      command.getMap().entrySet().forEach(filter::add);

      if (sync) {
         Collector<Map<Object, Object>> collector = commandAckCollector
               .createSegmentBasedCollector(command.getCommandInvocationId().getId(), filter.primaries.keySet(),
                     filter.backups, command.getTopologyId());
         CompletableFuture<Map<Object, Object>> localResult = new CompletableFuture<>();
         final Map<Object, Object> localEntries = filter.primaries.remove(localAddress);
         forwardToPrimaryOwners(command, filter, localResult).handle((map, throwable) -> {
            if (throwable != null) {
               collector.primaryException(throwable);
            } else {
               collector.primaryResult(map, true);
            }
            return null;
         });
         if (localEntries != null) {
            sendToBackups(command, localEntries, cacheTopology);
            CompletableFuture<?> remoteGet = checkRemoteGetIfNeeded(ctx, command, localEntries.keySet(), cacheTopology,
                                                                loadType == PRIMARY || loadType == OWNER);
            return makeStage(asyncInvokeNext(ctx, command, remoteGet))
                  .andHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
                     if (throwable != null) {
                        localResult.completeExceptionally(CompletableFutures.extractException(throwable));
                     } else {
                        //noinspection unchecked
                        localResult.complete((Map<Object, Object>) rv);
                     }
                     return asyncValue(collector.getFuture());
                  });
         } else {
            localResult.complete(command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? null : new HashMap<>());
            return asyncValue(collector.getFuture());
         }
      }

      final Map<Object, Object> localEntries = filter.primaries.remove(localAddress);
      forwardToPrimaryOwners(command, filter);
      if (localEntries != null) {
         sendToBackups(command, localEntries, cacheTopology);
         return asyncInvokeNext(ctx, command,
               checkRemoteGetIfNeeded(ctx, command, localEntries.keySet(), cacheTopology,
                     loadType == PRIMARY || loadType == OWNER));
      }
      return null;
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletableFuture<?> checkRemoteGetIfNeeded(
         InvocationContext ctx, C command, Set<Object> keys, LocalizedCacheTopology cacheTopology,
         boolean needsPreviousValue) {
      if (!needsPreviousValue) {
         for (Object key : keys) {
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null && cacheTopology.isWriteOwner(key)) {
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            }
         }
         return CompletableFutures.completedNull();
      }
      final List<CompletableFuture<?>> futureList = new ArrayList<>(keys.size());
      for (Object key : keys) {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null && cacheTopology.isWriteOwner(key)) {
            wrapKeyExternally(ctx, command, key, futureList);
         }
      }
      final int size = futureList.size();
      if (size == 0) {
         return CompletableFutures.completedNull();
      }
      CompletableFuture[] array = new CompletableFuture[size];
      futureList.toArray(array);
      return CompletableFuture.allOf(array);
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand> void wrapKeyExternally(InvocationContext ctx,
         C command, Object key, List<CompletableFuture<?>> futureList) {
      if (command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.CACHE_MODE_LOCAL)) {
         entryFactory.wrapExternalEntry(ctx, key, null, false, true);
      } else {
         GetCacheEntryCommand fakeGetCommand = cf.buildGetCacheEntryCommand(key, command.getFlagsBitSet());
         fakeGetCommand.setTopologyId(command.getTopologyId());
         futureList.add(remoteGet(ctx, fakeGetCommand, key, true).toCompletableFuture());
      }
   }

   private void forwardToPrimaryOwners(PutMapCommand command, PrimaryOwnerClassifier splitter) {
      for (Map.Entry<Address, Map<Object, Object>> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.NONE);
      }
   }

   private CompletableFuture<Map<Object, Object>> forwardToPrimaryOwners(PutMapCommand command,
         PrimaryOwnerClassifier splitter, CompletableFuture<Map<Object, Object>> localResult) {
      CompletableFuture<Map<Object, Object>> future = localResult;
      for (Map.Entry<Address, Map<Object, Object>> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         copy.setTopologyId(command.getTopologyId());
         CompletionStage<ValidResponse> remoteFuture = rpcManager.invokeCommand(entry.getKey(), copy,
                                                                                SingleResponseCollector.validOnly(),
                                                                                rpcManager.getSyncRpcOptions());
         future = remoteFuture.toCompletableFuture().thenCombine(future, TriangleDistributionInterceptor::mergeMaps);
      }
      return future;
   }

   // TODO: this should just override handleNonTxWriteCommand when functional commands will be triangelized
   private Object handleDataWriteCommand(InvocationContext context, AbstractDataWriteCommand command) {
      assert !context.isInTxScope();
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         //don't go through the triangle
         return invokeNext(context, command);
      }
      LocalizedCacheTopology topology = checkTopologyId(command);
      DistributionInfo distributionInfo = topology.getDistribution(command.getKey());

      if (distributionInfo.isPrimary()) {
         assert context.lookupEntry(command.getKey()) != null;
         return context.isOriginLocal() ?
               localPrimaryOwnerWrite(context, command, distributionInfo) :
               remotePrimaryOwnerWrite(context, command, distributionInfo);
      } else if (distributionInfo.isWriteBackup()) {
         return context.isOriginLocal() ?
               localWriteInvocation(context, command, distributionInfo) :
               remoteBackupOwnerWrite(context, command);
      } else {
         //always local!
         assert context.isOriginLocal();
         return localWriteInvocation(context, command, distributionInfo);
      }
   }

   private Object remoteBackupOwnerWrite(InvocationContext context, DataWriteCommand command) {
      CacheEntry entry = context.lookupEntry(command.getKey());
      if (entry == null) {
         if (command.loadType() == OWNER) {
            return asyncInvokeNext(context, command, remoteGet(context, command, command.getKey(), true));
         }
         entryFactory.wrapExternalEntry(context, command.getKey(), null, false, true);
      }
      return invokeNext(context, command);
   }

   private Object localPrimaryOwnerWrite(InvocationContext context, DataWriteCommand command,
         DistributionInfo distributionInfo) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         final DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         Collection<Address> backupOwners = distributionInfo.writeBackups();
         if (!dwCommand.isSuccessful() || backupOwners.isEmpty()) {
            if (trace) {
               log.tracef("Command %s not successful in primary owner.", id);
            }
            return rv;
         }
         final int topologyId = dwCommand.getTopologyId();
         final boolean sync = isSynchronous(dwCommand);
         if (sync || dwCommand.isReturnValueExpected()) {
            Collector<Object> collector = commandAckCollector.create(id.getId(),
                  sync ? backupOwners : Collections.emptyList(),
                  topologyId);
            //check the topology after registering the collector.
            //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
            checkTopologyId(topologyId, collector);
            collector.primaryResult(rv, true);
            sendToBackups(distributionInfo, dwCommand, backupOwners);
            return asyncValue(collector.getFuture());
         } else {
            sendToBackups(distributionInfo, dwCommand, backupOwners);
            return rv;
         }
      });
   }

   private Object remotePrimaryOwnerWrite(InvocationContext context, DataWriteCommand command,
         final DistributionInfo distributionInfo) {
      //we are the primary owner. we need to execute the command, check if successful, send to backups and reply to originator is needed.
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         final DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         Collection<Address> backupOwners = distributionInfo.writeBackups();
         if (!dwCommand.isSuccessful() || backupOwners.isEmpty()) {
            if (trace) {
               log.tracef("Command %s not successful in primary owner.", id);
            }
            return rv;
         }
         sendToBackups(distributionInfo, dwCommand, backupOwners);
         return rv;
      });
   }

   private void sendToBackups(DistributionInfo distributionInfo, DataWriteCommand command,
         Collection<Address> backupOwners) {
      CommandInvocationId id = command.getCommandInvocationId();
      int segmentId = distributionInfo.segmentId();
      if (trace) {
         log.tracef("Command %s send to backup owner %s.", id, backupOwners);
      }
      long sequenceNumber = triangleOrderManager.next(segmentId, command.getTopologyId());
      BackupWriteRpcCommand backupWriteRpcCommand = commandsFactory.buildBackupWriteRpcCommand(command);
      backupWriteRpcCommand.setSequence(sequenceNumber);
      if (trace) {
         log.tracef("Command %s got sequence %s for segment %s", id, sequenceNumber, segmentId);
      }
      // TODO Should we use sendToAll in replicated mode?
      // we must send the message only after the collector is registered in the map
      rpcManager.sendToMany(backupOwners, backupWriteRpcCommand, DeliverOrder.NONE);
   }

   private Object localWriteInvocation(InvocationContext context, DataWriteCommand command,
         DistributionInfo distributionInfo) {
      assert context.isOriginLocal();
      final CommandInvocationId invocationId = command.getCommandInvocationId();
      final boolean sync = isSynchronous(command);
      if (sync || command.isReturnValueExpected() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         final int topologyId = command.getTopologyId();
         Collector<Object> collector = commandAckCollector.create(invocationId.getId(),
               sync ? distributionInfo.writeBackups() : Collections.emptyList(),
               topologyId);
         //check the topology after registering the collector.
         //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
         checkTopologyId(topologyId, collector);
         forwardToPrimary(command, distributionInfo, collector);
         return asyncValue(collector.getFuture());
      } else {
         rpcManager.sendTo(distributionInfo.primary(), command, DeliverOrder.NONE);
         return null;
      }
   }

   private void forwardToPrimary(DataWriteCommand command, DistributionInfo distributionInfo,
         Collector<Object> collector) {
      CompletionStage<ValidResponse> remoteInvocation =
            rpcManager.invokeCommand(distributionInfo.primary(), command, SingleResponseCollector.validOnly(),
                                     rpcManager.getSyncRpcOptions());
      remoteInvocation.handle((response, throwable) -> {
         if (throwable != null) {
            collector.primaryException(CompletableFutures.extractException(throwable));
         } else {
            if (!response.isSuccessful()) {
               command.fail();
            }
            collector.primaryResult(response.getResponseValue(), response.isSuccessful());
         }
         return null;
      });
   }

   private void checkTopologyId(int topologyId, Collector<?> collector) {
      int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
      if (currentTopologyId != topologyId && topologyId != -1) {
         collector.primaryException(OutdatedTopologyException.INSTANCE);
         throw OutdatedTopologyException.INSTANCE;
      }
   }


   /**
    * Classifies the keys by primary owner (address => keys & segments) and backup owners (address => segments).
    * <p>
    * The first map is used to forward the command to the primary owner with the subset of keys.
    * <p>
    * The second map is used to initialize the {@link CommandAckCollector} to wait for the backups acknowledges.
    */
   private static class PrimaryOwnerClassifier {
      private final Map<Address, Collection<Integer>> backups;
      private final Map<Address, Map<Object, Object>> primaries;
      private final LocalizedCacheTopology cacheTopology;
      private final int entryCount;

      private PrimaryOwnerClassifier(LocalizedCacheTopology cacheTopology, int entryCount) {
         this.cacheTopology = cacheTopology;
         int memberSize = cacheTopology.getMembers().size();
         this.backups = new HashMap<>(memberSize);
         this.primaries = new HashMap<>(memberSize);
         this.entryCount = entryCount;
      }

      public void add(Map.Entry<Object, Object> entry) {
         int segment = cacheTopology.getSegment(entry.getKey());
         DistributionInfo distributionInfo = cacheTopology.getDistributionForSegment(segment);
         final Address primaryOwner = distributionInfo.primary();
         primaries.computeIfAbsent(primaryOwner, address -> new HashMap<>(entryCount))
               .put(entry.getKey(), entry.getValue());
         for (Address backup : distributionInfo.writeBackups()) {
            backups.computeIfAbsent(backup, address -> new HashSet<>(entryCount)).add(segment);
         }
      }

   }

   /**
    * A classifier used in the primary owner when handles a remote {@link PutMapCommand}.
    * <p>
    * It maps the backup owner address to the subset of keys.
    */
   private static class BackupOwnerClassifier {
      private final Map<Integer, Map<Object, Object>> perSegmentKeyValue;
      private final LocalizedCacheTopology cacheTopology;
      private final int entryCount;

      private BackupOwnerClassifier(LocalizedCacheTopology cacheTopology, int entryCount) {
         this.cacheTopology = cacheTopology;
         this.perSegmentKeyValue = new HashMap<>(cacheTopology.getReadConsistentHash().getNumSegments());
         this.entryCount = entryCount;
      }

      public void add(Map.Entry<Object, Object> entry) {
         perSegmentKeyValue
               .computeIfAbsent(cacheTopology.getSegment(entry.getKey()), address -> new HashMap<>(entryCount))
               .put(entry.getKey(), entry.getValue());
      }
   }

}
