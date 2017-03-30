package org.infinispan.interceptors.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.BackupPutMapRpcCommand;
import org.infinispan.commands.write.BackupWriteRpcCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
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
   private CommandAckCollector commandAckCollector;
   private CommandsFactory commandsFactory;
   private TriangleOrderManager triangleOrderManager;
   private Address localAddress;

   private static Map<Object, Object> mergeMaps(Map<Address, Response> responses, Map<Object, Object> resultMap) {
      //noinspection unchecked
      Map<Object, Object> remoteMap = (Map<Object, Object>) ((SuccessfulResponse) responses.values().iterator().next())
            .getResponseValue();
      return InfinispanCollections.mergeMaps(resultMap, remoteMap);
   }

   @Inject
   public void inject(CommandAckCollector commandAckCollector, CommandsFactory commandsFactory,
         TriangleOrderManager triangleOrderManager) {
      this.commandAckCollector = commandAckCollector;
      this.commandsFactory = commandsFactory;
      this.triangleOrderManager = triangleOrderManager;
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
      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command, cacheTopology);
      } else {
         if (command.isForwarded()) {
            return handleBackupForReadWriteManyCommand(ctx, command, putMapHelper, cacheTopology);
         } else {
            return handleRemotePutMapCommand(ctx, command, cacheTopology);
         }
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

   private Object handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command, LocalizedCacheTopology cacheTopology) {
      Map<Object, Object> completedResults = null;
      Map<Object, Object> localEntries = command.getMap();
      BackupOwnerClassifier backupFilter = new BackupOwnerClassifier(cacheTopology, localEntries.size());
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         completedResults = checkInvocationRecords(ctx, command, localEntries.entrySet(), backupFilter::add, null, putMapHelper);
      } else {
         localEntries.forEach(backupFilter::add);
      }
      Map<Object, Object> finalCompletedResults = completedResults;
      sendToBackups(command, cacheTopology, finalCompletedResults, backupFilter);

      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         Map<Object, Object> allLocalResults = (Map<Object, Object>) rv;
         if (finalCompletedResults != null && allLocalResults != null) {
            allLocalResults.putAll(finalCompletedResults);
         }
         return allLocalResults;
      });
   }

   private void sendToBackups(PutMapCommand command, LocalizedCacheTopology cacheTopology, Map<Object, Object> completedResults, BackupOwnerClassifier filter) {
      int topologyId = command.getTopologyId();
      for (Map.Entry<Integer, Map<Object, Object>> entry : filter.perSegmentKeyValue.entrySet()) {
         int segmentId = entry.getKey();
         DistributionInfo distributionInfo = cacheTopology.getDistributionForSegment(segmentId);
         Collection<Address> backups = distributionInfo.writeBackups();
         if (backups.isEmpty()) {
            // Only the primary owner. Other segments may have more than one owner, e.g. during rebalance.
            continue;
         }
         Map<Object, Object> map = entry.getValue();
         long sequence = triangleOrderManager.next(segmentId, topologyId);
         BackupPutMapRpcCommand backupPutMapRpcCommand = commandsFactory.buildBackupPutMapRpcCommand(command);
         backupPutMapRpcCommand.setMap(map);
         backupPutMapRpcCommand.setTopologyId(command.getTopologyId());
         if (completedResults != null && distributionInfo.isAnyWriteBackupNonReader()) {
            Map<Object, Object> completedResultsPerSegment = completedResults.entrySet().stream().filter(e -> map.containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            backupPutMapRpcCommand.setProvidedResults(completedResultsPerSegment);
         }
         backupPutMapRpcCommand.setSequence(sequence);
         if (trace) {
            log.tracef("Command %s got sequence %s for segment %s", command.getCommandInvocationId(), segmentId,
                  sequence);
         }
         rpcManager.sendToMany(backups, backupPutMapRpcCommand, DeliverOrder.NONE);
      }
   }

   private Object handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command, LocalizedCacheTopology cacheTopology) {
      //local command. we need to split by primary owner to send the command to them
      final TrianglePrimaryOwnerClassifier filter = new TrianglePrimaryOwnerClassifier(cacheTopology, command.getMap().size(), putMapHelper);
      final boolean sync = isSynchronous(command);

      // TODO: handle CACHE_MODE_LOCAL

      command.getMap().entrySet().forEach(filter::add);
      CompletableFuture<Map<Object, Object>> localResult;
      Collector<Map<Object, Object>> collector;
      final Map<Object, Object> localEntries = filter.primaries.remove(localAddress);

      if (sync) {
         collector = commandAckCollector.createMultiKeyCollector(command.getCommandInvocationId().getId(),
               filter.backups, command.getTopologyId());
         localResult = new CompletableFuture<>();

         forwardToPrimaryOwners(command, filter, localResult).handle((map, throwable) -> {
            if (throwable != null) {
               collector.primaryException(throwable);
            } else {
               collector.primaryResult(map, true);
            }
            return null;
         });
         collector.getFuture().thenRun(() -> invocationManager.notifyCompleted(command.getCommandInvocationId(), filter.keysBySegment()));
      } else {
         forwardToPrimaryOwners(command, filter);
         localResult = null;
         collector = null;
      }

      if (localEntries != null) {
         Map<Object, Object> completedResults = null;
         BackupOwnerClassifier backupFilter = new BackupOwnerClassifier(cacheTopology, localEntries.size());
         if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
            completedResults = checkInvocationRecords(ctx, command, localEntries.entrySet(), backupFilter::add, null, putMapHelper);
         } else {
            localEntries.forEach(backupFilter::add);
         }
         Map<Object, Object> finalCompletedResults = completedResults;
         sendToBackups(command, cacheTopology, finalCompletedResults, backupFilter);

         PutMapCommand localCopy = new PutMapCommand(command, false).withMap(localEntries);
         if (sync) {
            return invokeNextAndHandle(ctx, localCopy, (rCtx, rCommand, rv, throwable) -> {
               if (throwable != null) {
                  localResult.completeExceptionally(CompletableFutures.extractException(throwable));
               } else {
                  //noinspection unchecked
                  Map<Object, Object> allLocalResults = (Map<Object, Object>) rv;
                  if (finalCompletedResults != null && allLocalResults != null) {
                     allLocalResults.putAll(finalCompletedResults);
                  }
                  localResult.complete(allLocalResults);
               }
               return asyncValue(collector.getFuture());
            });
         } else {
            return invokeNext(ctx, localCopy);
         }
      } else if (sync) {
         localResult.complete(null);
         return asyncValue(collector.getFuture());
      } else {
         return null;
      }
   }

   private void forwardToPrimaryOwners(PutMapCommand command,
         TrianglePrimaryOwnerClassifier splitter) {
      for (Map.Entry<Address, Map<Object, Object>> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.NONE);
      }
   }

   private CompletableFuture<Map<Object, Object>> forwardToPrimaryOwners(PutMapCommand command,
       TrianglePrimaryOwnerClassifier splitter, CompletableFuture<Map<Object, Object>> localResult) {
      CompletableFuture<Map<Object, Object>> future = localResult;
      for (Map.Entry<Address, Map<Object, Object>> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         CompletableFuture<Map<Address, Response>> remoteFuture = rpcManager
               .invokeRemotelyAsync(Collections.singleton(entry.getKey()), copy, defaultSyncOptions);
         future = remoteFuture.thenCombine(future, TriangleDistributionInterceptor::mergeMaps);
      }
      return future;
   }

   // TODO: this should just override handleNonTxWriteCommand when functional commands will be triangelized
   private Object handleDataWriteCommand(InvocationContext context, AbstractDataWriteCommand command) {
      assert !context.isInTxScope();
      Object key = command.getKey();
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         //don't go through the triangle
         if (context.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(context, key, null, false, true);
         }
         command.setAuthoritative(true);
         return invokeNext(context, command);
      }

      LocalizedCacheTopology topology = checkTopologyId(command.getTopologyId());
      DistributionInfo distributionInfo = topology.getDistribution(key);

      if (distributionInfo.isPrimary()) {
         return context.isOriginLocal() ?
               localPrimaryOwnerWrite(context, command, distributionInfo) :
               remotePrimaryOwnerWrite(context, command, distributionInfo);
      } else if (distributionInfo.isWriteBackup()) {
         if (context.isOriginLocal()) {
            return localWriteInvocation(context, command, distributionInfo);
         } else {
            return handleBackupWrite(context, command, distributionInfo);
         }
      } else {
         //always local!
         assert context.isOriginLocal();
         return localWriteInvocation(context, command, distributionInfo);
      }
   }

   private Object localPrimaryOwnerWrite(InvocationContext context, DataWriteCommand command,
         DistributionInfo distributionInfo) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ByRef<Object> returnValue = new ByRef<>(null);
         if (checkInvocationRecord(context, command, distributionInfo, returnValue, this::localPrimarySendToBackupsAndReturn)) {
            return returnValue.get();
         }
      }

      command.setAuthoritative(true);
      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         if (dwCommand.isSuccessful()) {
            // If the command has been successful, we're going to update backups and
            // we don't have to worry about ISPN-3918
            return localPrimarySendToBackupsAndReturn(context, dwCommand, distributionInfo, rv);
         } else {
            return handleUnsuccessfulWriteOnPrimary(rCtx, rv, dwCommand.getKey(), distributionInfo);
         }
      });
   }

   private Object localPrimarySendToBackupsAndReturn(InvocationContext ctx, DataWriteCommand command, DistributionInfo distributionInfo, Object rv) {
      final CommandInvocationId id = command.getCommandInvocationId();
      Collection<Address> backupOwners = distributionInfo.writeBackups();
      if (!command.isSuccessful() || backupOwners.isEmpty()) {
         if (trace) {
            log.tracef("Command %s not successful in primary owner.", id);
         }
         return rv;
      }
      boolean provideResult = !command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) && distributionInfo.isAnyWriteBackupNonReader();
      final int topologyId = command.getTopologyId();
      if (isSynchronous(command) || command.isReturnValueExpected()) {
         Collector<Object> collector = commandAckCollector.create(id.getId(), backupOwners, topologyId);
         //check the topology after registering the collector.
         //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
         checkTopologyId(topologyId, collector);
         collector.primaryResult(rv, true);
         sendToBackups(distributionInfo, command, backupOwners, provideResult, rv);
         collector.getFuture().thenRun(() -> invocationManager.notifyCompleted(command.getCommandInvocationId(), command.getKey(), distributionInfo.segmentId()));
         return asyncValue(collector.getFuture());
      } else {
         sendToBackups(distributionInfo, command, backupOwners, provideResult, rv);
         return rv;
      }
   }

   private Object remotePrimaryOwnerWrite(InvocationContext context, DataWriteCommand command,
         final DistributionInfo distributionInfo) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         ByRef<Object> returnValue = new ByRef<>(null);
         if (checkInvocationRecord(context, command, distributionInfo, returnValue, this::remotePrimarySendToBackupsAndReturn)) {
            return returnValue.get();
         }
      }

      command.setAuthoritative(true);
      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         if (rCommand.isSuccessful()) {
            return remotePrimarySendToBackupsAndReturn(rCtx, dwCommand, distributionInfo, rv);
         } else {
            return handleUnsuccessfulWriteOnPrimary(rCtx, rv, dwCommand.getKey(), distributionInfo);
         }
      });
   }

   private Object remotePrimarySendToBackupsAndReturn(InvocationContext ctx, DataWriteCommand command, DistributionInfo distributionInfo, Object rv) {
      Collection<Address> backupOwners = distributionInfo.writeBackups();
      if (!command.isSuccessful() || backupOwners.isEmpty()) {
         if (trace) {
            log.tracef("Command %s not successful in primary owner.", command.getCommandInvocationId());
         }
         return rv;
      }
      boolean provideResult = !command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) && distributionInfo.isAnyWriteBackupNonReader();
      sendToBackups(distributionInfo, command, backupOwners, provideResult, rv);
      return rv;
   }

   private void sendToBackups(DistributionInfo distributionInfo, DataWriteCommand command,
                              Collection<Address> backupOwners, boolean provideResult, Object result) {
      CommandInvocationId id = command.getCommandInvocationId();
      int segmentId = distributionInfo.segmentId();
      if (trace) {
         log.tracef("Command %s send to backup owner %s.", id, backupOwners);
      }
      long sequenceNumber = triangleOrderManager.next(segmentId, command.getTopologyId());

      BackupWriteRpcCommand backupWriteRpcCommand = commandsFactory.buildBackupWriteRpcCommand(command);
      backupWriteRpcCommand.setSequence(sequenceNumber);
      if (provideResult) {
         backupWriteRpcCommand.setProvidedResult(result);
      }
      if (trace) {
         log.tracef("Command %s got sequence %s for segment %s", id, sequenceNumber, segmentId);
      }
      // we must send the message only after the collector is registered in the map
      rpcManager.sendToMany(backupOwners, backupWriteRpcCommand, DeliverOrder.NONE);
   }

   private Object localWriteInvocation(InvocationContext context, DataWriteCommand command,
         DistributionInfo distributionInfo) {
      assert context.isOriginLocal();
      final CommandInvocationId invocationId = command.getCommandInvocationId();
      if (isSynchronous(command) || command.isReturnValueExpected() && !command
            .hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         final int topologyId = command.getTopologyId();
         Collector<Object> collector = commandAckCollector
               .create(invocationId.getId(), distributionInfo.writeBackups(), topologyId);
         //check the topology after registering the collector.
         //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
         checkTopologyId(topologyId, collector);
         forwardToPrimary(command, distributionInfo, collector);
         collector.getFuture().thenRun(() -> invocationManager.notifyCompleted(
               command.getCommandInvocationId(), command.getKey(), distributionInfo.segmentId()));
         return asyncValue(collector.getFuture());
      } else {
         rpcManager.sendTo(distributionInfo.primary(), command, DeliverOrder.NONE);
         return null;
      }
   }

   private void forwardToPrimary(DataWriteCommand command, DistributionInfo distributionInfo,
         Collector<Object> collector) {
      CompletableFuture<Map<Address, Response>> remoteInvocation = rpcManager
            .invokeRemotelyAsync(Collections.singletonList(distributionInfo.primary()), command, defaultSyncOptions);
      remoteInvocation.handle((responses, throwable) -> {
         if (throwable != null) {
            collector.primaryException(CompletableFutures.extractException(throwable));
         } else {
            ValidResponse response = (ValidResponse) responses.values().iterator().next();
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

   private static class TrianglePrimaryOwnerClassifier extends PrimaryOwnerClassifier<Map<Object, Object>, Map.Entry<Object, Object>> {
      protected final Map<Address, Collection<Integer>> backups;
      private final int entryCount;

      protected TrianglePrimaryOwnerClassifier(LocalizedCacheTopology cacheTopology, int entryCount, WriteManyCommandHelper<?, Map<Object, Object>, Map.Entry<Object, Object>> helper) {
         super(cacheTopology, entryCount, helper);
         this.backups = new HashMap<>(cacheTopology.getMembers().size());
         this.entryCount = entryCount;
      }

      @Override
      public void add(Object key, Map.Entry<Object, Object> item, DistributionInfo distributionInfo) {
         super.add(key, item, distributionInfo);
         for (Address backup : distributionInfo.writeBackups()) {
            backups.computeIfAbsent(backup, address -> new HashSet<>(entryCount)).add(distributionInfo.segmentId());
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
         add(entry.getKey(), entry.getValue());
      }

      public void add(Object key, Object value) {
         perSegmentKeyValue
               .computeIfAbsent(cacheTopology.getSegment(key), address -> new HashMap<>(entryCount))
               .put(key, value);
      }
   }

}
