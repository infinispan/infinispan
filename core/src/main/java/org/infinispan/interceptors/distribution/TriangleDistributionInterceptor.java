package org.infinispan.interceptors.distribution;

import static org.infinispan.commands.VisitableCommand.LoadType.OWNER;
import static org.infinispan.commands.VisitableCommand.LoadType.PRIMARY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
import org.infinispan.commands.write.BackupPutMapRcpCommand;
import org.infinispan.commands.write.BackupWriteRcpCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PrimaryMultiKeyAckCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.TriangleAckInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.topology.CacheTopology;
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
 * stored in L1). If it isn't available, a remote request is made.
 * The {@link DataWriteCommand} is performed as follow:
 * <ul>
 * <li>The command if forwarded to the primary owner of the key.</li>
 * <li>The primary owner locks the key and executes the operation; sends the {@link BackupWriteRcpCommand} to the backup
 * owners; releases the lock; sends the {@link PrimaryAckCommand} back to the originator.</li>
 * <li>The backup owner applies the update and sends a {@link BackupAckCommand} back to the originator.</li>
 * <li>The originator collects the ack from all the owners and returns.</li>
 * </ul>
 * The {@link PutMapCommand} is performed in a similar way:
 * <ul>
 * <li>The subset of the map is split by primary owner.</li>
 * <li>The primary owner locks the key and executes the command; splits the keys by backup owner and send them;
 * releases the locks and sends the {@link PrimaryMultiKeyAckCommand} back to the originator.</li>
 * <li>The backup owner applies the update and sends back the {@link BackupMultiKeyAckCommand} to the originator.</li>
 * <li>The originator collects all the acknowledges from all owners and returns.</li>
 * </ul>
 * The acknowledges management is done in the {@link TriangleAckInterceptor} by the {@link CommandAckCollector}.
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
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return handleLocalPutMapCommand(ctx, command);
      } else {
         return handleRemotePutMapCommand(ctx, command);
      }
   }

   @Override
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataWriteCommand(ctx, command);
   }

   private BasicInvocationStage handleRemotePutMapCommand(InvocationContext ctx, PutMapCommand command) {
      CacheTopology cacheTopology = checkTopologyId(command);
      final ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      final VisitableCommand.LoadType loadType = command.loadType();

      if (command.isForwarded() || ch.getNumOwners() == 1) {
         //backup & remote || no backups
         return invokeNextAsync(ctx, command,
               checkRemoteGetIfNeeded(ctx, command, command.getMap().keySet(), ch, loadType == OWNER));
      }
      //primary, we need to send the command to the backups ordered!
      sendToBackups(command, command.getMap(), ch);
      return invokeNextAsync(ctx, command,
            checkRemoteGetIfNeeded(ctx, command, command.getMap().keySet(), ch, loadType == OWNER));
   }

   private void sendToBackups(PutMapCommand command, Map<Object, Object> entries, ConsistentHash ch) {
      BackupOwnerClassifier filter = new BackupOwnerClassifier(ch);
      entries.entrySet().forEach(filter::add);
      for (Map.Entry<Address, Map<Object, Object>> entry : filter.perBackupKeyValue.entrySet()) {
         Map<Integer, Long> segmentsAndSequences = new HashMap<>();
         Map<Object, Object> map = entry.getValue();
         map.keySet().forEach(key -> segmentsAndSequences.put(cdl.getSegmentForKey(key), -1L));
         triangleOrderManager.next(segmentsAndSequences, command.getTopologyId());
         BackupPutMapRcpCommand backupPutMapRcpCommand = commandsFactory.buildBackupPutMapRcpCommand(command);
         backupPutMapRcpCommand.setMap(map);
         backupPutMapRcpCommand.setSegmentsAndSequences(segmentsAndSequences);
         rpcManager.sendTo(entry.getKey(), backupPutMapRcpCommand, DeliverOrder.NONE);
      }
   }

   private BasicInvocationStage handleLocalPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      //local command. we need to split by primary owner to send the command to them
      final CacheTopology cacheTopology = checkTopologyId(command);
      final ConsistentHash consistentHash = cacheTopology.getWriteConsistentHash();
      final PrimaryOwnerClassifier filter = new PrimaryOwnerClassifier(consistentHash);
      final boolean sync = isSynchronous(command);
      final VisitableCommand.LoadType loadType = command.loadType();

      command.getMap().entrySet().forEach(filter::add);

      if (sync) {
         commandAckCollector
               .createMultiKeyCollector(command.getCommandInvocationId(), filter.primaries.keySet(), filter.backups,
                     command.getTopologyId());
         final Map<Object, Object> localEntries = filter.primaries.remove(localAddress);
         forwardToPrimaryOwners(command, filter);
         if (localEntries != null) {
            sendToBackups(command, localEntries, consistentHash);
            return invokeNextAsync(ctx, command,
                  checkRemoteGetIfNeeded(ctx, command, localEntries.keySet(), consistentHash,
                        loadType == PRIMARY || loadType == OWNER)).handle((rCtx, rCommand, rv, t) -> {
               PutMapCommand cmd = (PutMapCommand) rCommand;
               if (t != null) {
                  commandAckCollector.completeExceptionally(cmd.getCommandInvocationId(), t, cmd.getTopologyId());
               } else {
                  //noinspection unchecked
                  commandAckCollector.multiKeyPrimaryAck(cmd.getCommandInvocationId(), localAddress,
                        (Map<Object, Object>) rv, cmd.getTopologyId());
               }
            });
         }
         return invokeNext(ctx, command).exceptionally((rCtx, rCommand, t) -> {
            PutMapCommand cmd = (PutMapCommand) rCommand;
            commandAckCollector.completeExceptionally(cmd.getCommandInvocationId(), t, cmd.getTopologyId());
            assert t != null;
            throw t;
         });
      }

      final Map<Object, Object> localEntries = filter.primaries.remove(localAddress);
      forwardToPrimaryOwners(command, filter);
      if (localEntries != null) {
         sendToBackups(command, localEntries, consistentHash);
         return invokeNextAsync(ctx, command,
               checkRemoteGetIfNeeded(ctx, command, localEntries.keySet(), consistentHash,
                     loadType == PRIMARY || loadType == OWNER));
      }
      return invokeNext(ctx, command);
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand> CompletableFuture<?> checkRemoteGetIfNeeded(
         InvocationContext ctx, C command, Set<Object> keys, ConsistentHash consistentHash,
         boolean needsPreviousValue) {
      if (!needsPreviousValue) {
         for (Object key : keys) {
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null && consistentHash.isKeyLocalToNode(localAddress, key)) {
               entryFactory.wrapExternalEntry(ctx, key, null, true);
            }
         }
         return CompletableFutures.completedNull();
      }
      final List<CompletableFuture<?>> futureList = new ArrayList<>(keys.size());
      for (Object key : keys) {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null && consistentHash.isKeyLocalToNode(localAddress, key)) {
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
         entryFactory.wrapExternalEntry(ctx, key, null, true);
      } else {
         GetCacheEntryCommand fakeGetCommand = cf.buildGetCacheEntryCommand(key, command.getFlagsBitSet());
         fakeGetCommand.setTopologyId(command.getTopologyId());
         futureList.add(remoteGet(ctx, fakeGetCommand, key, true));
      }
   }

   private void forwardToPrimaryOwners(PutMapCommand command, PrimaryOwnerClassifier splitter) {
      for (Map.Entry<Address, Map<Object, Object>> entry : splitter.primaries.entrySet()) {
         PutMapCommand copy = new PutMapCommand(command, false);
         copy.setMap(entry.getValue());
         rpcManager.sendTo(entry.getKey(), copy, DeliverOrder.NONE);
      }
   }

   // TODO: this should just override handleNonTxWriteCommand when functional commands will be triangelized
   private BasicInvocationStage handleDataWriteCommand(InvocationContext context, AbstractDataWriteCommand command) {
      assert !context.isInTxScope();
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
         //don't go through the triangle
         return invokeNext(context, command);
      }
      final CacheTopology topology = checkTopologyId(command);
      DistributionInfo distributionInfo = new DistributionInfo(command.getKey(), topology.getWriteConsistentHash(),
            localAddress);

      switch (distributionInfo.ownership()) {
         case PRIMARY:
            assert context.lookupEntry(command.getKey()) != null;
            return primaryOwnerWrite(context, command, distributionInfo);
         case BACKUP:
            if (context.isOriginLocal()) {
               return localWriteInvocation(context, command, distributionInfo);
            } else {
               CacheEntry entry = context.lookupEntry(command.getKey());
               if (entry == null) {
                  if (command.loadType() == OWNER) {
                     return invokeNextAsync(context, command, remoteGet(context, command, command.getKey(), true));
                  }
                  entryFactory.wrapExternalEntry(context, command.getKey(), null, true);
               }
               return invokeNext(context, command);
            }
         case NON_OWNER:
            //always local!
            assert context.isOriginLocal();
            return localWriteInvocation(context, command, distributionInfo);
      }
      throw new IllegalStateException();
   }

   private BasicInvocationStage primaryOwnerWrite(InvocationContext context, DataWriteCommand command,
         final DistributionInfo distributionInfo) {
      //we are the primary owner. we need to execute the command, check if successful, send to backups and reply to originator is needed.
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }

      return invokeNext(context, command).thenAccept((rCtx, rCommand, rv) -> {
         final DataWriteCommand dwCommand = (DataWriteCommand) rCommand;
         final CommandInvocationId id = dwCommand.getCommandInvocationId();
         if (!dwCommand.isSuccessful()) {
            if (trace) {
               log.tracef("Command %s not successful in primary owner.", id);
            }
            return;
         }
         if (distributionInfo.owners().size() > 1) {
            Collection<Address> backupOwners = distributionInfo.backups();
            if (rCtx.isOriginLocal() && (isSynchronous(dwCommand) || dwCommand.isReturnValueExpected())) {
               commandAckCollector.create(id, rv, distributionInfo.owners(), dwCommand.getTopologyId());
               //check the topology after registering the collector.
               //if we don't, the collector may wait forever (==timeout) for non-existing acknowledges.
               checkTopologyId(dwCommand);
            }
            if (trace) {
               log.tracef("Command %s send to backup owner %s.", dwCommand.getCommandInvocationId(), backupOwners);
            }
            long sequenceNumber = triangleOrderManager.next(distributionInfo.getSegmentId(), dwCommand.getTopologyId());
            BackupWriteRcpCommand backupWriteRcpCommand = commandsFactory.buildBackupWriteRcpCommand(dwCommand);
            backupWriteRcpCommand.setSequence(sequenceNumber);
            // we must send the message only after the collector is registered in the map
            rpcManager.sendToMany(backupOwners, backupWriteRcpCommand, DeliverOrder.NONE);
         }
      });
   }

   private BasicInvocationStage localWriteInvocation(InvocationContext context, DataWriteCommand command,
         DistributionInfo distributionInfo) {
      assert context.isOriginLocal();
      final CommandInvocationId invocationId = command.getCommandInvocationId();
      if ((isSynchronous(command) || command.isReturnValueExpected()) &&
            !command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ)) {
         commandAckCollector.create(invocationId, distributionInfo.owners(), command.getTopologyId());
      }
      if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
      }
      rpcManager.sendTo(distributionInfo.primary(), command, DeliverOrder.NONE);
      return returnWith(null);
   }

   /**
    * Classifies the keys by primary owner (address => keys & segments) and backup owners (address => segments).
    * <p>
    * The first map is used to forward the command to the primary owner with the subset of keys.
    * <p>
    * The second map is used to initialize the {@link CommandAckCollector} to wait for the backups acknowledges.
    */
   private static class PrimaryOwnerClassifier {
      private final Map<Address, Collection<Integer>> backups = new HashMap<>();
      private final Map<Address, Map<Object, Object>> primaries = new HashMap<>();
      private final ConsistentHash consistentHash;

      private PrimaryOwnerClassifier(ConsistentHash consistentHash) {
         this.consistentHash = consistentHash;
      }

      public void add(Map.Entry<Object, Object> entry) {
         final int segment = consistentHash.getSegment(entry.getKey());
         final Iterator<Address> iterator = consistentHash.locateOwnersForSegment(segment).iterator();
         final Address primaryOwner = iterator.next();
         primaries.computeIfAbsent(primaryOwner, address -> new HashMap<>()).put(entry.getKey(), entry.getValue());
         while (iterator.hasNext()) {
            Address backup = iterator.next();
            backups.computeIfAbsent(backup, address -> new HashSet<>()).add(segment);
         }
      }

   }

   /**
    * A classifier used in the primary owner when handles a remote {@link PutMapCommand}.
    * <p>
    * It maps the backup owner address to the subset of keys.
    */
   private static class BackupOwnerClassifier {
      private final Map<Address, Map<Object, Object>> perBackupKeyValue = new HashMap<>();
      private final ConsistentHash consistentHash;

      private BackupOwnerClassifier(ConsistentHash consistentHash) {
         this.consistentHash = consistentHash;
      }

      public void add(Map.Entry<Object, Object> entry) {
         Iterator<Address> iterator = consistentHash.locateOwners(entry.getKey()).iterator();
         iterator.next();
         while (iterator.hasNext()) {
            perBackupKeyValue.computeIfAbsent(iterator.next(), address -> new HashMap<>())
                  .put(entry.getKey(), entry.getValue());
         }
      }
   }

}
