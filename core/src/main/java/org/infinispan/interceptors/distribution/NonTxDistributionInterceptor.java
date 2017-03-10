package org.infinispan.interceptors.distribution;

import static org.infinispan.interceptors.distribution.MergingCompletableFuture.moveListItemsToFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Non-transactional interceptor used by distributed caches that support concurrent writes.
 * It is implemented based on lock forwarding. E.g.
 * - 'k' is written on node A, owners(k)={B,C}
 * - A forwards the given command to B
 * - B acquires a lock on 'k' then it forwards it to the remaining owners: C
 * - C applies the change and returns to B (no lock acquisition is needed)
 * - B applies the result as well, releases the lock and returns the result of the operation to A.
 * <p>
 * Note that even though this introduces an additional RPC (the forwarding), it behaves very well in
 * conjunction with
 * consistent-hash aware hotrod clients which connect directly to the lock owner.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 * @since 8.1
 */
public class NonTxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(NonTxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final PutMapHelper putMapHelper = new PutMapHelper(this::createRemoteCallback);
   private final ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper(this::createRemoteCallback);
   private final ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper(this::createRemoteCallback);
   private final WriteOnlyManyEntriesHelper writeOnlyManyEntriesHelper = new WriteOnlyManyEntriesHelper(this::createRemoteCallback);
   private final WriteOnlyManyHelper writeOnlyManyHelper = new WriteOnlyManyHelper(this::createRemoteCallback);

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws
         Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command)
         throws Throwable {
      return handleReadWriteManyCommand(ctx, command, putMapHelper);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                  WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteOnlyManyCommand(ctx, command, writeOnlyManyEntriesHelper);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx,
                                           WriteOnlyManyCommand command) throws Throwable {
      return handleWriteOnlyManyCommand(ctx, command, writeOnlyManyHelper);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx,
                                           ReadWriteManyCommand command) throws Throwable {
      return handleReadWriteManyCommand(ctx, command, readWriteManyHelper);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                  ReadWriteManyEntriesCommand command) throws Throwable {
      return handleReadWriteManyCommand(ctx, command, readWriteManyEntriesHelper);
   }

   private <C extends WriteCommand, Container, Item> Object handleWriteOnlyManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper) throws Exception {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      if (ctx.isOriginLocal()) {
         Collection<Item> items = helper.getItems(command);
         if (items.isEmpty()) {
            return null;
         }
         PrimaryOwnerClassifier<Container, Item> filter = new PrimaryOwnerClassifier<>(cacheTopology, items.size(), helper);
         items.forEach(filter::add);
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(filter.primaries.size());
         if (isSynchronous(command)) {
            allFuture.thenRun(() -> invocationManager.notifyCompleted(command.getCommandInvocationId(), filter.keysBySegment()));
         }

         Container localItems = filter.primaries.remove(rpcManager.getAddress());
         if (localItems != null) {
            C localCommand = helper.copyForLocal(command, localItems);
            handleLocalEntriesForWriteOnlyManyCommand(ctx, localCommand, helper, cacheTopology, helper.asCollection(localItems), allFuture);
         }

         for (Entry<Address, Container> remote : filter.primaries.entrySet()) {
            // copyForLocal means that we already have the entries filtered
            C copy = helper.copyForLocal(command, remote.getValue());
            rpcManager.invokeCommand(remote.getKey(), copy, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
                  .whenComplete(allFuture);
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         if (helper.isForwarded(command)) {
            for (Item item : helper.getItems(command)) {
               Object key = helper.item2key(item);
               CacheEntry entry = ctx.lookupEntry(key);
               if (entry == null) {
                  entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               }
            }
            return invokeNext(ctx, command);
         } else {
            CountDownCompletableFuture allFuture = new CountDownCompletableFuture(1);
            handleLocalEntriesForWriteOnlyManyCommand(ctx, command, helper, cacheTopology,
                  helper.getItems(command), allFuture);
            return asyncValue(allFuture);
         }
      }
   }

   private <C extends WriteCommand, Container, Item> void handleLocalEntriesForWriteOnlyManyCommand(
         InvocationContext ctx, C localCommand, WriteManyCommandHelper<C, Container, Item> helper,
         LocalizedCacheTopology cacheTopology, Collection<Item> localItems,
         CountDownCompletableFuture allFuture) throws Exception {
      Map<Address, Container> backupItems = new HashMap<>();

      for (Item item : localItems) {
         DistributionInfo info = cacheTopology.getDistribution(helper.item2key(item));
         for (Address backup : info.writeBackups()) {
            helper.accumulate(backupItems.computeIfAbsent(backup, b -> helper.newContainer()), item);
         }
      }
      Map<Address, C> backupCommands = backupItems.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
               C backupCommand = helper.copyForLocal(localCommand, e.getValue());
               helper.setForwarded(backupCommand);
               return backupCommand;
            }));

      InvocationFinallyAction handler = createLocalInvocationHandler(backupCommands, allFuture, (f, r) -> {});
      invokeNextAndFinally(ctx, localCommand, handler);
   }

   private <C extends WriteCommand, Container, Item> Object handleReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper) throws Exception {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      if (ctx.isOriginLocal()) {
         Collection<Item> items = helper.getItems(command);
         if (items.isEmpty()) {
            return null;
         }
         PrimaryOwnerClassifier<Container, Item> filter = new PrimaryOwnerClassifier<>(cacheTopology, items.size(), helper);
         items.forEach(filter::add);
         Object[] results = null;
         if (!command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
            results = new Object[items.size()];
         }
         MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(filter.primaries.size(),
               results, helper::transformResult);
         if (isSynchronous(command)) {
            allFuture.thenRun(() -> invocationManager.notifyCompleted(command.getCommandInvocationId(), filter.keysBySegment()));
         }
         MutableInt offset = new MutableInt();

         Container localItems = filter.primaries.remove(rpcManager.getAddress());
         if (localItems != null) {
            C localCommand = helper.copyForLocal(command, localItems);
            handleLocalEntriesForReadWriteManyCommand(ctx, command, localCommand, helper, cacheTopology, helper.asCollection(localItems), allFuture, offset);
         }

         for (Entry<Address, Container> remote : filter.primaries.entrySet()) {
            forwardReadWriteManyToPrimary(command, helper, remote.getValue(), allFuture, offset, remote.getKey());
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         if (helper.isForwarded(command)) {
            return handleBackupForReadWriteManyCommand(ctx, command, helper, cacheTopology);
         } else {
            Object[] results = null;
            if (!command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
               results = new Object[helper.getItems(command).size()];
            }
            MergingCompletableFuture<Object> allFuture
                  = new MergingCompletableFuture<>(1, results, helper::transformResult);
            handleLocalEntriesForReadWriteManyCommand(ctx, command, command, helper, cacheTopology,
                  helper.getItems(command), allFuture, new MutableInt());
            return asyncValue(allFuture);
         }
      }
   }

   // handle entries for which this node is a primary owner
   private <C extends WriteCommand, Container, Item> void handleLocalEntriesForReadWriteManyCommand(
         InvocationContext ctx, C command, C localCommand, WriteManyCommandHelper<C, Container, Item> helper,
         LocalizedCacheTopology cacheTopology, Collection<Item> localItems,
         MergingCompletableFuture<Object> allFuture, MutableInt offset) throws Exception {
      Map<Address, Container> backupItems = new HashMap<>();

      // TODO: optimize lambdas
      Consumer<Item> addToBackup = item -> {
         Object key = helper.item2key(item);
         DistributionInfo info = cacheTopology.getDistribution(key);
         for (Address backup : info.writeBackups()) {
            helper.accumulate(backupItems.computeIfAbsent(backup, b -> helper.newContainer()), item);
         }
      };
      if (localCommand.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         checkInvocationRecords(ctx, command);
      } else {
         localItems.forEach(addToBackup);
      }
      Map<Address, C> backupCommands = backupItems.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e -> {
               C cmd = helper.copyForLocal(command, e.getValue());
               helper.setForwarded(cmd);
               return cmd;
            }));

      int size = localItems.size();
      if (size == 0) {
         allFuture.countDown();
         return;
      }
      final int myOffset = offset.value;
      offset.value += size;

      InvocationFinallyAction handler = createLocalInvocationHandler(backupCommands, allFuture, moveListItemsToFuture(myOffset));
      invokeNextAndFinally(ctx, localCommand, handler);
      // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
      // calls complete.
   }

   private <C extends WriteCommand, Container, Item> void forwardReadWriteManyToPrimary(
         C command, WriteManyCommandHelper<C, Container, Item> helper,
         Container items, MergingCompletableFuture<Object> allFuture,
         MutableInt offset, Address member) {
      final int myOffset = offset.value;
      C copy = helper.copyForLocal(command, items);
      int size = helper.getItems(copy).size();
      offset.value += size;
      if (size <= 0) {
         allFuture.countDown();
         return;
      }

      // Send the command to primary owner
      SingletonMapResponseCollector collector = SingletonMapResponseCollector.validOnly();
      rpcManager.invokeCommand(member, copy, collector, rpcManager.getSyncRpcOptions())
            .whenComplete((responses, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               } else {
                  // FIXME Dan: The response cannot be a CacheNotFoundResponse at this point
                  SuccessfulResponse response = getSuccessfulResponseOrFail(responses, allFuture,
                        rsp -> allFuture.completeExceptionally(OutdatedTopologyException.INSTANCE));
                  if (response == null) {
                     return;
                  }
                  Object responseValue = response.getResponseValue();
                  moveListItemsToFuture(responseValue, allFuture, myOffset);
                  allFuture.countDown();
               }
            });
   }

   protected <C extends WriteCommand, Container, Item> Object handleBackupForReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper, LocalizedCacheTopology cacheTopology) throws Exception {
      Map<Address, List<Object>> fetchedKeys = new HashMap<>();
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         handleBackupWrite(ctx, command, key, cacheTopology::getDistribution, (ctx2, cmd2, k, info) -> {
            fetchedKeys.computeIfAbsent(info.primary(), a -> new ArrayList<>()).add(k);
            return null;
         });
      }
      // all keys for backup have been handled
      if (fetchedKeys.isEmpty()) {
         if (command.completedKeys().findAny().isPresent()) {
            Container container = helper.newContainer();
            for (Item item : helper.getItems(command)) {
               Object key = helper.item2key(item);
               if (!command.isCompleted(key)) {
                  helper.accumulate(container, item);
               }
            }
            return invokeNext(ctx, helper.copyForLocal(command, container));
         } else {
            return invokeNext(ctx, command);
         }
      }
      ClusteredGetAllFuture allFuture = new ClusteredGetAllFuture(fetchedKeys.size());
      for (Entry<Address, List<Object>> pair : fetchedKeys.entrySet()) {
         List<Object> keys = pair.getValue();
         ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(keys, FlagBitSets.WITH_INVOCATION_RECORDS, null);
         rpcManager.invokeCommand(pair.getKey(), clusteredGetAllCommand, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions())
               .whenComplete((response, throwable) -> {
                  if (throwable != null) {
                     allFuture.completeExceptionally(throwable);
                     return;
                  }
                  if (!response.isSuccessful()) {
                     allFuture.completeExceptionally(unexpected(response));
                  }
                  handleClusteredGetAllResponse(ctx, keys, (SuccessfulResponse) response, allFuture, true);
               });
      }
      return asyncValue(allFuture).thenApply(ctx, command, (rCtx, rCommand, nil) -> {
         C writeCommand = (C) rCommand;
         for (Entry<Address, List<Object>> pair : fetchedKeys.entrySet()) {
            for (Object key : pair.getValue()) {
               handleBackupWrite(rCtx, writeCommand, key, null, BaseDistributionInterceptor::unexpectedRetrieval);
            }
         }
         Container container = helper.newContainer();
         for (Item item : helper.getItems(writeCommand)) {
            Object key = helper.item2key(item);
            if (!writeCommand.isCompleted(key)) {
               helper.accumulate(container, item);
            } else {
               // We need to not execute the command on given key but persist the history on local node
               rCtx.lookupEntry(key).setChanged(true);
               writeCommand.setCompleted(key, false);
            }
         }
         return invokeNext(rCtx, helper.copyForLocal(writeCommand, container));
      });
   }

   private <C extends WriteCommand, F extends CountDownCompletableFuture>
   InvocationFinallyAction createLocalInvocationHandler(
         Map<Address, C> backupCommands,
         F allFuture,
         BiConsumer<F, Object> returnValueConsumer) {
      return (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            returnValueConsumer.accept(allFuture, rv);

            for (Entry<Address, C> backup : backupCommands.entrySet()) {
               // rCommand is the original command
               C backupCopy = backup.getValue();
               if (isSynchronous(backupCopy)) {
                  allFuture.increment();
                  rpcManager.invokeCommand(backup.getKey(), backupCopy, SingleResponseCollector.validOnly(),
                        rpcManager.getSyncRpcOptions()).whenComplete(allFuture);
               } else {
                  rpcManager.sendTo(backup.getKey(), backupCopy, DeliverOrder.PER_SENDER);
               }
            }
            allFuture.countDown();
         } catch (Throwable t) {
            allFuture.completeExceptionally(t);
         }
      };
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   private final static class MutableInt {
      public int value;
   }

   private <C extends WriteCommand> Object writeManyRemoteCallback(WriteManyCommandHelper<C, ?, ?> helper, InvocationContext ctx, C command, Object rv) {
      LocalizedCacheTopology topology = checkTopologyId(command);
      ConsistentHash writeCH = topology.getWriteConsistentHash();
      // We have already checked that the command topology is actual, so we can assume that we really are primary owner
      Map<Address, Set<Integer>> backups = backupOwnersOfSegments(writeCH);
      if (backups.isEmpty()) {
         return rv;
      }
      boolean isSync = isSynchronous(command);
      CompletableFuture[] futures = isSync ? new CompletableFuture[backups.size()] : null;
      int future = 0;
      for (Entry<Address, Set<Integer>> backup : backups.entrySet()) {
         C copy = helper.copyForBackup(command, writeCH, backup.getValue());
         if (isSync) {
            futures[future++] = rpcManager.invokeCommand(backup.getKey(), copy, SingleResponseCollector.validOnly(),
                        rpcManager.getSyncRpcOptions())
                  .toCompletableFuture();
         } else {
            rpcManager.sendTo(backup.getKey(), copy, DeliverOrder.PER_SENDER);
         }
      }
      return isSync ? asyncValue(CompletableFuture.allOf(futures).thenApply(nil -> rv)) : rv;
   }

   private <C extends WriteCommand> InvocationSuccessFunction createRemoteCallback(WriteManyCommandHelper<C, ?, ?> helper) {
      return (ctx, command, rv) -> writeManyRemoteCallback(helper, ctx, (C) command, rv);
   }
}
