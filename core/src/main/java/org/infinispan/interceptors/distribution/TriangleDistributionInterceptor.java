package org.infinispan.interceptors.distribution;

import static org.infinispan.commands.VisitableCommand.LoadType.OWNER;
import static org.infinispan.commands.VisitableCommand.LoadType.PRIMARY;
import static org.infinispan.util.TriangleFunctionsUtil.filterBySegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.triangle.BackupNoopCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.ExceptionSyncInvocationStage;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.util.CacheTopologyUtil;
import org.infinispan.util.TriangleFunctionsUtil;
import org.infinispan.util.concurrent.CommandAckCollector;
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
 * executes the operation; sends the {@link BackupWriteCommand} to the backup owners; releases the lock; sends the
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
public class TriangleDistributionInterceptor extends BaseDistributionInterceptor {

   private static final Log log = LogFactory.getLog(TriangleDistributionInterceptor.class);
   @Inject CommandAckCollector commandAckCollector;
   @Inject CommandsFactory commandsFactory;
   @Inject TriangleOrderManager triangleOrderManager;
   private Address localAddress;

   @Start
   public void start() {
      localAddress = rpcManager.getAddress();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
      return handleSingleKeyWriteCommand(ctx,command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command, commandsFactory::buildSingleKeyBackupWriteCommand);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return ctx.isOriginLocal() ?
            handleLocalManyKeysCommand(ctx, command,
                  TriangleFunctionsUtil::copy,
                  TriangleFunctionsUtil::mergeHashMap,
                  HashMap::new,
                  commandsFactory::buildPutMapBackupWriteCommand) :
            handleRemoteManyKeysCommand(ctx, command,
                  PutMapCommand::isForwarded,
                  commandsFactory::buildPutMapBackupWriteCommand);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return ctx.isOriginLocal() ?
            handleLocalManyKeysCommand(ctx, command,
                  TriangleFunctionsUtil::copy,
                  TriangleFunctionsUtil::voidMerge,
                  () -> null,
                  commandsFactory::buildMultiEntriesFunctionalBackupWriteCommand) :
            handleRemoteManyKeysCommand(ctx, command,
                  WriteOnlyManyEntriesCommand::isForwarded,
                  commandsFactory::buildMultiEntriesFunctionalBackupWriteCommand);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return ctx.isOriginLocal() ?
            handleLocalManyKeysCommand(ctx, command,
                  TriangleFunctionsUtil::copy,
                  TriangleFunctionsUtil::voidMerge,
                  () -> null,
                  commandsFactory::buildMultiKeyFunctionalBackupWriteCommand) :
            handleRemoteManyKeysCommand(ctx, command,
                  WriteOnlyManyCommand::isForwarded,
                  commandsFactory::buildMultiKeyFunctionalBackupWriteCommand);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return ctx.isOriginLocal() ?
            handleLocalManyKeysCommand(ctx, command,
                  TriangleFunctionsUtil::copy,
                  TriangleFunctionsUtil::mergeList,
                  LinkedList::new,
                  commandsFactory::buildMultiKeyFunctionalBackupWriteCommand) :
            handleRemoteManyKeysCommand(ctx, command,
                  ReadWriteManyCommand::isForwarded,
                  commandsFactory::buildMultiKeyFunctionalBackupWriteCommand);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command)
         throws Throwable {
      return ctx.isOriginLocal() ?
            handleLocalManyKeysCommand(ctx, command,
                  TriangleFunctionsUtil::copy,
                  TriangleFunctionsUtil::mergeList,
                  LinkedList::new,
                  commandsFactory::buildMultiEntriesFunctionalBackupWriteCommand) :
            handleRemoteManyKeysCommand(ctx, command,
                  ReadWriteManyEntriesCommand::isForwarded,
                  commandsFactory::buildMultiEntriesFunctionalBackupWriteCommand);
   }

   private <R, C extends WriteCommand> Object handleLocalManyKeysCommand(InvocationContext ctx, C command,
         SubsetCommandCopy<C> commandCopy,
         MergeResults<R> mergeResults,
         Supplier<R> emptyResult,
         MultiKeyBackupBuilder<C> backupBuilder) {

      //local command. we need to split by primary owner to send the command to them
      final LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      final PrimaryOwnerClassifier filter = new PrimaryOwnerClassifier(cacheTopology, command.getAffectedKeys());

      return isSynchronous(command) ?
            syncLocalManyKeysWrite(ctx, command, cacheTopology, filter, commandCopy, mergeResults, emptyResult,
                  backupBuilder) :
            asyncLocalManyKeysWrite(ctx, command, cacheTopology, filter, commandCopy, backupBuilder);
   }

   private <C extends WriteCommand> Object handleRemoteManyKeysCommand(InvocationContext ctx, C command,
         Predicate<C> isBackup,
         MultiKeyBackupBuilder<C> backupBuilder) {
      return isBackup.test(command) ?
            remoteBackupManyKeysWrite(ctx, command, InfinispanCollections.toObjectSet(command.getAffectedKeys())) :
            remotePrimaryManyKeysWrite(ctx, command, InfinispanCollections.toObjectSet(command.getAffectedKeys()),
                  backupBuilder);
   }

   private <C extends WriteCommand> Object remoteBackupManyKeysWrite(InvocationContext ctx, C command,
         Set<Object> keys) {
      //backup & remote
      final LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      return asyncInvokeNext(ctx, command,
            checkRemoteGetIfNeeded(ctx, command, keys, cacheTopology, command.loadType() == OWNER));
   }

   private <C extends WriteCommand> Object remotePrimaryManyKeysWrite(InvocationContext ctx, C command,
         Set<Object> keys,
         MultiKeyBackupBuilder<C> backupBuilder) {
      //primary owner & remote
      final LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      //primary, we need to send the command to the backups ordered!
      sendToBackups(command, keys, cacheTopology, backupBuilder);
      return asyncInvokeNext(ctx, command,
            checkRemoteGetIfNeeded(ctx, command, keys, cacheTopology, command.loadType() == OWNER));
   }

   private <R, C extends WriteCommand> Object syncLocalManyKeysWrite(InvocationContext ctx, C command,
         LocalizedCacheTopology cacheTopology,
         PrimaryOwnerClassifier filter,
         SubsetCommandCopy<C> commandCopy,
         MergeResults<R> mergeResults,
         Supplier<R> emptyResult,
         MultiKeyBackupBuilder<C> backupBuilder) {
      //local & sync
      final Set<Object> localKeys = filter.primaries.remove(localAddress);
      Collector<R> collector = commandAckCollector.createSegmentBasedCollector(command.getCommandInvocationId().getId(),
                                                                               filter.backups, command.getTopologyId());
      CompletableFuture<R> localResult = new CompletableFuture<>();
      try {
         forwardToPrimaryOwners(command, filter, localResult, mergeResults, commandCopy)
               .handle((result, throwable) -> {
                  if (throwable != null) {
                     collector.primaryException(throwable);
                  } else {
                     collector.primaryResult(result, true);
                  }
                  return null;
               });
      } catch (Throwable t) {
         collector.primaryException(t);
      }
      if (localKeys != null) {
         return makeStage(invokeNextWriteManyKeysInPrimary(ctx, command, localKeys, cacheTopology, commandCopy,
                                                           backupBuilder))
               .andHandle(ctx, command, (rCtx, rCommand, rv, throwable) -> {
                  if (throwable != null) {
                     localResult.completeExceptionally(CompletableFutures.extractException(throwable));
                  } else {
                     //noinspection unchecked
                     localResult.complete((R) rv);
                  }
                  return asyncValue(collector.getFuture());
               });
      } else {
         localResult.complete(command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? null : emptyResult.get());
         return asyncValue(collector.getFuture());
      }
   }

   private <C extends WriteCommand> Object asyncLocalManyKeysWrite(InvocationContext ctx, C command,
         LocalizedCacheTopology cacheTopology,
         PrimaryOwnerClassifier filter,
         SubsetCommandCopy<C> commandCopy,
         MultiKeyBackupBuilder<C> backupBuilder) {
      //local & async
      final Set<Object> localKeys = filter.primaries.remove(localAddress);
      forwardToPrimaryOwners(command, filter, commandCopy);
      return localKeys != null ?
            invokeNextWriteManyKeysInPrimary(ctx, command, localKeys, cacheTopology, commandCopy, backupBuilder) :
            null; //no local keys to handle
   }

   private <C extends WriteCommand> Object invokeNextWriteManyKeysInPrimary(InvocationContext ctx, C command,
         Set<Object> keys,
         LocalizedCacheTopology cacheTopology,
         SubsetCommandCopy<C> commandCopy,
         MultiKeyBackupBuilder<C> backupBuilder) {
      try {
         sendToBackups(command, keys, cacheTopology, backupBuilder);
         final VisitableCommand.LoadType loadType = command.loadType();
         C primaryCmd = commandCopy.copySubset(command, keys);
         return asyncInvokeNext(ctx, primaryCmd,
                                checkRemoteGetIfNeeded(ctx, primaryCmd, keys, cacheTopology,
                                                       loadType == PRIMARY || loadType == OWNER));
      } catch (Throwable t) {
         // Wrap marshalling exception in an invocation stage
         return new ExceptionSyncInvocationStage(t);
      }
   }

   private <C extends WriteCommand> void sendToBackups(C command, Collection<Object> keysToSend,
         LocalizedCacheTopology cacheTopology, MultiKeyBackupBuilder<C> backupBuilder) {
      int topologyId = command.getTopologyId();
      for (Map.Entry<Integer, Collection<Object>> entry : filterBySegment(cacheTopology, keysToSend).entrySet()) {
         int segmentId = entry.getKey();
         Collection<Address> backups = cacheTopology.getSegmentDistribution(segmentId).writeBackups();
         if (backups.isEmpty()) {
            // Only the primary owner. Other segments may have more than one owner, e.g. during rebalance.
            continue;
         }
         long sequence = triangleOrderManager.next(segmentId, topologyId);
         try {
            BackupWriteCommand backupCommand = backupBuilder.build(command, entry.getValue(), sequence, segmentId);
            if (log.isTraceEnabled()) {
               log.tracef("Command %s got sequence %s for segment %s", command.getCommandInvocationId(), segmentId,
                          sequence);
            }
            rpcManager.sendToMany(backups, backupCommand, DeliverOrder.NONE);
         } catch (Throwable t) {
            sendBackupNoopCommand(command, backups, segmentId, sequence);
            throw t;
         }
      }
   }

   private <C extends WriteCommand> void forwardToPrimaryOwners(C command, PrimaryOwnerClassifier splitter,
         SubsetCommandCopy<C> commandCopy) {
      for (Map.Entry<Address, Set<Object>> entry : splitter.primaries.entrySet()) {
         C copy = commandCopy.copySubset(command, entry.getValue());
         copy.setTopologyId(command.getTopologyId());
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.NONE);
      }
   }

   private <R, C extends WriteCommand> CompletableFuture<R> forwardToPrimaryOwners(C command,
         PrimaryOwnerClassifier splitter,
         CompletableFuture<R> localResult,
         MergeResults<R> mergeResults,
         SubsetCommandCopy<C> commandCopy) {
      CompletableFuture<R> future = localResult;
      for (Map.Entry<Address, Set<Object>> entry : splitter.primaries.entrySet()) {
         C copy = commandCopy.copySubset(command, entry.getValue());
         copy.setTopologyId(command.getTopologyId());
         CompletionStage<ValidResponse> remoteFuture = rpcManager.invokeCommand(entry.getKey(), copy,
               SingleResponseCollector.validOnly(),
               rpcManager.getSyncRpcOptions());
         future = remoteFuture.toCompletableFuture().thenCombine(future, mergeResults);
      }
      return future;
   }

   private <C extends DataWriteCommand> Object handleSingleKeyWriteCommand(InvocationContext context, C command,
         BackupBuilder<C> backupBuilder) {
      assert !context.isInTxScope();
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         //don't go through the triangle
         return invokeNext(context, command);
      }
      LocalizedCacheTopology topology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      DistributionInfo distributionInfo = topology.getSegmentDistribution(command.getSegment());

      if (distributionInfo.isPrimary()) {
         assert context.lookupEntry(command.getKey()) != null;
         return context.isOriginLocal() ?
               localPrimaryOwnerWrite(context, command, distributionInfo, backupBuilder) :
               remotePrimaryOwnerWrite(context, command, distributionInfo, backupBuilder);
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
            return asyncInvokeNext(context, command, remoteGetSingleKey(context, command, command.getKey(), true));
         }
         entryFactory.wrapExternalEntry(context, command.getKey(), null, false, true);
      }
      return invokeNext(context, command);
   }

   private <C extends DataWriteCommand> Object localPrimaryOwnerWrite(InvocationContext context, C command,
         DistributionInfo distributionInfo, BackupBuilder<C> backupBuilder) {
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         //noinspection unchecked
         final C dwCommand = (C) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         Collection<Address> backupOwners = distributionInfo.writeBackups();
         if (!dwCommand.shouldReplicate(rCtx, true) || backupOwners.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Not sending command %s to backups", id);
            }
            return rv;
         }
         final int topologyId = dwCommand.getTopologyId();
         final boolean sync = isSynchronous(dwCommand);
         if (sync || dwCommand.isReturnValueExpected()) {
            Collector<Object> collector = commandAckCollector.create(id.getId(),
                  sync ? backupOwners : Collections.emptyList(),
                  topologyId);
            try {
               //check the topology after registering the collector.
               //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
               checkTopologyId(topologyId, collector);
               sendToBackups(distributionInfo.segmentId(), dwCommand, backupOwners, backupBuilder);
               collector.primaryResult(rv, true);
            } catch (Throwable t) {
               collector.primaryException(t);
            }
            return asyncValue(collector.getFuture());
         } else {
            sendToBackups(distributionInfo.segmentId(), dwCommand, backupOwners, backupBuilder);
            return rv;
         }
      });
   }

   private void sendBackupNoopCommand(WriteCommand command, Collection<Address> targets, int segment, long sequence) {
      BackupNoopCommand noopCommand = commandsFactory.buildBackupNoopCommand(command, sequence, segment);
      rpcManager.sendToMany(targets, noopCommand, DeliverOrder.NONE);
   }

   private <C extends DataWriteCommand> Object remotePrimaryOwnerWrite(InvocationContext context, C command,
         final DistributionInfo distributionInfo, BackupBuilder<C> backupBuilder) {
      //we are the primary owner. we need to execute the command, check if successful, send to backups and reply to originator is needed.
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNextThenApply(context, command, (rCtx, rCommand, rv) -> {
         //noinspection unchecked
         final C dwCommand = (C) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         Collection<Address> backupOwners = distributionInfo.writeBackups();
         // Note we have to replicate even if the command says not to with triangle if the command
         // didn't originate at the primary since the backup may be the originator
         if (!dwCommand.isSuccessful() || backupOwners.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Command %s not replicating from primary owner.", id);
            }
            return rv;
         }
         sendToBackups(distributionInfo.segmentId(), dwCommand, backupOwners, backupBuilder);
         return rv;
      });
   }

   private <C extends DataWriteCommand> void sendToBackups(int segmentId, C command, Collection<Address> backupOwners,
         BackupBuilder<C> backupBuilder) {
      CommandInvocationId id = command.getCommandInvocationId();
      if (log.isTraceEnabled()) {
         log.tracef("Command %s send to backup owner %s.", id, backupOwners);
      }
      long sequenceNumber = triangleOrderManager.next(segmentId, command.getTopologyId());
      try {
         BackupWriteCommand backupCommand = backupBuilder.build(command, sequenceNumber, segmentId);
         if (log.isTraceEnabled()) {
            log.tracef("Command %s got sequence %s for segment %s", id, sequenceNumber, segmentId);
         }
         // TODO Should we use sendToAll in replicated mode?
         // we must send the message only after the collector is registered in the map
         rpcManager.sendToMany(backupOwners, backupCommand, DeliverOrder.NONE);
      } catch (Throwable t) {
         sendBackupNoopCommand(command, backupOwners, segmentId, sequenceNumber);
         throw t;
      }
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
         try {
            //check the topology after registering the collector.
            //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
            checkTopologyId(topologyId, collector);
            forwardToPrimary(command, distributionInfo, collector);
            return asyncValue(collector.getFuture());
         } catch (Throwable t) {
            collector.primaryException(t);
            throw t;
         }
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

   private <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletionStage<?> checkRemoteGetIfNeeded(
         InvocationContext ctx, C command, Set<Object> keys, LocalizedCacheTopology cacheTopology,
         boolean needsPreviousValue) {
      List<Object> remoteKeys = null;
      for (Object key : keys) {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null && cacheTopology.isWriteOwner(key)) {
            if (!needsPreviousValue || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.CACHE_MODE_LOCAL)) {
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            } else {
               if (remoteKeys == null) {
                  remoteKeys = new ArrayList<>();
               }
               remoteKeys.add(key);
            }
         }
      }
      return remoteKeys != null ? remoteGetMany(ctx, command, remoteKeys) : CompletableFutures.completedNull();
   }

   private void checkTopologyId(int topologyId, Collector<?> collector) {
      int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
      if (currentTopologyId != topologyId && topologyId != -1) {
         collector.primaryException(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY);
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
   }

   private interface SubsetCommandCopy<T> {
      T copySubset(T t, Collection<Object> keys);
   }

   private interface MergeResults<T> extends BiFunction<ValidResponse, T, T> {
   }

   private interface BackupBuilder<C> {
      BackupWriteCommand build(C command, long sequenceId, int segmendId);
   }

   private interface MultiKeyBackupBuilder<C> {
      BackupWriteCommand build(C command, Collection<Object> keys, long sequenceId, int segmendId);
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
      private final Map<Address, Set<Object>> primaries;
      private final LocalizedCacheTopology cacheTopology;
      private final int entryCount;

      private PrimaryOwnerClassifier(LocalizedCacheTopology cacheTopology, Collection<?> keys) {
         this.cacheTopology = cacheTopology;
         int memberSize = cacheTopology.getMembers().size();
         this.backups = new HashMap<>(memberSize);
         this.primaries = new HashMap<>(memberSize);
         Set<Object> distinctKeys = new HashSet<>(keys);
         this.entryCount = distinctKeys.size();
         distinctKeys.forEach(this::check);
      }

      private void check(Object key) {
         int segment = cacheTopology.getSegment(key);
         DistributionInfo distributionInfo = cacheTopology.getSegmentDistribution(segment);
         final Address primaryOwner = distributionInfo.primary();
         primaries.computeIfAbsent(primaryOwner, address -> new HashSet<>(entryCount))
               .add(key);
         for (Address backup : distributionInfo.writeBackups()) {
            backups.computeIfAbsent(backup, address -> new HashSet<>(entryCount)).add(segment);
         }
      }

   }
}
