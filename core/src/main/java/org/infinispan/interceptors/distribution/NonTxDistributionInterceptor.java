package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
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
import org.infinispan.util.CacheTopologyUtil;

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

   private final PutMapHelper putMapHelper = new PutMapHelper(this::createRemoteCallback);
   private final ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper(this::createRemoteCallback);
   private final ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper(this::createRemoteCallback);
   private final WriteOnlyManyEntriesHelper writeOnlyManyEntriesHelper = new WriteOnlyManyEntriesHelper(this::createRemoteCallback);
   private final WriteOnlyManyHelper writeOnlyManyHelper = new WriteOnlyManyHelper(this::createRemoteCallback);

   private Map<Address, IntSet> primaryOwnersOfSegments(ConsistentHash ch) {
      Map<Address, IntSet> map = new HashMap<>(ch.getMembers().size());
      for (Address member : ch.getMembers()) {
         Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
         if (!segments.isEmpty()) {
            map.put(member, IntSets.from(segments));
         }
      }
      return map;
   }

   // we're assuming that this function runs on the primary owner of the given segments
   private Map<Address, IntSet> backupOwnersOfSegments(LocalizedCacheTopology topology, IntSet segments) {
      Map<Address, IntSet> map = new HashMap<>(topology.getMembers().size() * 3 / 2);
      if (topology.getReadConsistentHash().isReplicated()) {
         // Use writeOwners to exclude zero-capacity members
         Collection<Address> writeOwners = topology.getSegmentDistribution(0).writeOwners();
         for (Address writeOwner : writeOwners) {
            if (!writeOwner.equals(topology.getLocalAddress())) {
               map.put(writeOwner, segments);
            }
         }
      } else {
         int numSegments = topology.getNumSegments();
         for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
            int segment = iter.nextInt();
            Collection<Address> backupOwners = topology.getSegmentDistribution(segment).writeBackups();
            for (Address backupOwner : backupOwners) {
               if (!backupOwner.equals(topology.getLocalAddress())) {
                  map.computeIfAbsent(backupOwner, o -> IntSets.mutableEmptySet(numSegments)).set(segment);
               }
            }
         }
      }
      return map;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws
         Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitIracPutKeyValueCommand(InvocationContext ctx, IracPutKeyValueCommand command) {
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
      // TODO: due to possible repeating of the operation (after OutdatedTopologyException is thrown)
      // it is possible that the function will be applied multiple times on some of the nodes.
      // There is no general solution for this ATM; proper solution will probably record CommandInvocationId
      // in the entry, and implement some housekeeping
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, IntSet> segmentMap = primaryOwnersOfSegments(ch);
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(segmentMap.size());

         // Go through all members, for this node invokeNext (if this node is an owner of some keys),
         // for the others (that own some keys) issue a remote call.
         // Everything is finished when allFuture is completed
         for (Entry<Address, IntSet> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            IntSet segments = pair.getValue();
            handleSegmentsForWriteOnlyManyCommand(ctx, command, helper, allFuture, member, segments, cacheTopology);
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         // check that we have all the data we need
         return handleRemoteWriteOnlyManyCommand(ctx, command, helper);
      }
   }

   private <C extends WriteCommand, Container, Item> void handleSegmentsForWriteOnlyManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper,
         CountDownCompletableFuture allFuture, Address member, IntSet segments, LocalizedCacheTopology topology) {
      if (member.equals(rpcManager.getAddress())) {
         Container myItems = filterAndWrap(ctx, command, segments, helper);

         C localCommand = helper.copyForLocal(command, myItems);
         localCommand.setTopologyId(command.getTopologyId());
         // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
         // calls complete.
         invokeNextAndFinally(ctx, localCommand,
                              createLocalInvocationHandler(allFuture, segments, helper, (f, rv) -> {}, topology));
         return;
      }

      C copy = helper.copyForPrimary(command, topology, segments);
      copy.setTopologyId(command.getTopologyId());
      int size = helper.getItems(copy).size();
      if (size <= 0) {
         allFuture.countDown();
         return;
      }

      SingletonMapResponseCollector collector = SingletonMapResponseCollector.validOnly();
      rpcManager.invokeCommand(member, copy, collector, rpcManager.getSyncRpcOptions())
            .whenComplete((responseMap, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               } else {
                  // FIXME Dan: The response cannot be a CacheNotFoundResponse at this point
                  if (getSuccessfulResponseOrFail(responseMap, allFuture,
                        rsp -> allFuture.completeExceptionally(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY)) == null) {
                     return;
                  }
                  allFuture.countDown();
               }
            });
   }

   private <C extends WriteCommand, Item> Object handleRemoteWriteOnlyManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, ?, Item> helper) {
      for (Object key : command.getAffectedKeys()) {
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
      }

      if (helper.shouldRegisterRemoteCallback(command)) {
         return invokeNextThenApply(ctx, command, helper.getRemoteCallback());
      } else {
         return invokeNext(ctx, command);
      }
   }

   private <C extends WriteCommand, Container, Item> Container filterAndWrap(
         InvocationContext ctx, C command, IntSet segments,
         WriteManyCommandHelper<C, Container, Item> helper) {
      // Filter command keys/entries into the collection, and wrap null for those that are not in context yet
      Container myItems = helper.newContainer();
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         if (segments.contains(keyPartitioner.getSegment(key))) {
            helper.accumulate(myItems, item);
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null) {
               // executed only be write-only commands
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            }
         }
      }
      return myItems;
   }

   protected <C extends WriteCommand, Container, Item> Object handleReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Item, Container> helper) throws Exception {
      // TODO: due to possible repeating of the operation (after OutdatedTopologyException is thrown)
      //  it is possible that the function will be applied multiple times on some of the nodes.
      //  There is no general solution for this ATM; proper solution will probably record CommandInvocationId
      //  in the entry, and implement some housekeeping
      LocalizedCacheTopology topology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      ConsistentHash ch = topology.getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, IntSet> segmentMap = primaryOwnersOfSegments(ch);
         Object[] results = null;
         if (!command.hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES)) {
            results = new Object[helper.getItems(command).size()];
         }
         MergingCompletableFuture<Object> allFuture
               = new MergingCompletableFuture<>(segmentMap.size(), results, helper::transformResult);
         MutableInt offset = new MutableInt();

         // Go through all members, for this node invokeNext (if this node is an owner of some keys),
         // for the others (that own some keys) issue a remote call.
         // Everything is finished when allFuture is completed
         for (Entry<Address, IntSet> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            IntSet segments = pair.getValue();
            if (member.equals(rpcManager.getAddress())) {
               handleLocalSegmentsForReadWriteManyCommand(ctx, command, helper, allFuture, offset, segments, topology);
            } else {
               handleRemoteSegmentsForReadWriteManyCommand(command, helper, allFuture, offset, member, segments, topology);
            }
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         return handleRemoteReadWriteManyCommand(ctx, command, helper);
      }
   }

   private <C extends WriteCommand, Container, Item> void handleLocalSegmentsForReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper,
         MergingCompletableFuture<Object> allFuture, MutableInt offset, IntSet segments,
         LocalizedCacheTopology topology) {
      Container myItems = helper.newContainer();
      List<Object> remoteKeys = null;
      // Filter command keys/entries into the collection, and record remote retrieval for those that are not
      // in the context yet
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         if (segments.contains(keyPartitioner.getSegment(key))) {
            helper.accumulate(myItems, item);
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null) {
               // this should be a rare situation, so we don't mind being a bit ineffective with the remote gets
               if (command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.CACHE_MODE_LOCAL)) {
                  entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               } else {
                  if (remoteKeys == null) {
                     remoteKeys = new ArrayList<>();
                  }
                  remoteKeys.add(key);
               }
            }
         }
      }

      CompletionStage<Void> retrievals = remoteKeys != null ? remoteGetMany(ctx, command, remoteKeys) : null;
      int size = helper.containerSize(myItems);
      if (size == 0) {
         allFuture.countDown();
         return;
      }
      final int myOffset = offset.value;
      offset.value += size;

      C localCommand = helper.copyForLocal(command, myItems);
      localCommand.setTopologyId(command.getTopologyId());
      InvocationFinallyAction<C> handler =
            createLocalInvocationHandler(allFuture, segments, helper, MergingCompletableFuture.moveListItemsToFuture(myOffset), topology);
      // It's safe to ignore the invocation stages below, because handleRemoteSegmentsForReadWriteManyCommand
      // does not touch the context.
      if (retrievals == null) {
         invokeNextAndFinally(ctx, localCommand, handler);
      } else {
         // We must wait until all retrievals finish before proceeding with the local command
         Object result = asyncInvokeNext(ctx, command, retrievals);
         makeStage(result).andFinally(ctx, command, handler);
      }
      // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
      // calls complete.
   }

   private <C extends WriteCommand, Item> void handleRemoteSegmentsForReadWriteManyCommand(
         C command, WriteManyCommandHelper<C, ?, Item> helper, MergingCompletableFuture<Object> allFuture,
         MutableInt offset, Address member, IntSet segments, LocalizedCacheTopology topology) {
      final int myOffset = offset.value;
      // TODO: here we iterate through all entries - is the ReadOnlySegmentAwareMap really worth it?
      C copy = helper.copyForPrimary(command, topology, segments);
      copy.setTopologyId(command.getTopologyId());
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
                        rsp -> allFuture.completeExceptionally(OutdatedTopologyException.RETRY_NEXT_TOPOLOGY));
                  if (response == null) {
                     return;
                  }
                  Object responseValue = response.getResponseValue();
                  MergingCompletableFuture.moveListItemsToFuture(responseValue, allFuture, myOffset);
                  allFuture.countDown();
               }
            });
   }

   private <C extends WriteCommand, Item> Object handleRemoteReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, ?, Item> helper) throws Exception {
      List<Object> remoteKeys = null;
      // check that we have all the data we need
      for (Object key : command.getAffectedKeys()) {
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null) {
            // this should be a rare situation, so we don't mind being a bit ineffective with the remote gets
            if (command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.CACHE_MODE_LOCAL)) {
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            } else {
               if (remoteKeys == null) {
                  remoteKeys = new ArrayList<>();
               }
               remoteKeys.add(key);
            }
         }
      }

      Object result;
      if (remoteKeys != null) {
         result = asyncInvokeNext(ctx, command, remoteGetMany(ctx, command, remoteKeys));
      } else {
         result = invokeNext(ctx, command);
      }
      if (helper.shouldRegisterRemoteCallback(command)) {
         return makeStage(result).thenApply(ctx, command, helper.getRemoteCallback());
      } else {
         return result;
      }
   }

   private <C extends WriteCommand, F extends CountDownCompletableFuture, Item>
   InvocationFinallyAction<C> createLocalInvocationHandler(
         F allFuture, IntSet segments, WriteManyCommandHelper<C, ?, Item> helper,
         BiConsumer<F, Object> returnValueConsumer, LocalizedCacheTopology topology) {
      return (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            returnValueConsumer.accept(allFuture, rv);

            Map<Address, IntSet> backupOwners = backupOwnersOfSegments(topology, segments);
            for (Entry<Address, IntSet> backup : backupOwners.entrySet()) {
               // rCommand is the original command
               C backupCopy = helper.copyForBackup(rCommand, topology, backup.getKey(), backup.getValue());
               backupCopy.setTopologyId(rCommand.getTopologyId());
               if (helper.getItems(backupCopy).isEmpty()) continue;
               Address backupOwner = backup.getKey();
               if (isSynchronous(backupCopy)) {
                  allFuture.increment();
                  rpcManager.invokeCommand(backupOwner, backupCopy, SingleResponseCollector.validOnly(),
                                           rpcManager.getSyncRpcOptions())
                        .whenComplete((response, remoteThrowable) -> {
                           if (remoteThrowable != null) {
                              allFuture.completeExceptionally(remoteThrowable);
                           } else {
                              allFuture.countDown();
                           }
                        });
               } else {
                  rpcManager.sendTo(backupOwner, backupCopy, DeliverOrder.PER_SENDER);
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

   private <C extends WriteCommand> Object writeManyRemoteCallback(WriteManyCommandHelper<C , ?, ?> helper,InvocationContext ctx, C command, Object rv) {
      // The node running this method must be primary owner for all the command's keys
      // Check that the command topology is actual, so we can assume that we really are primary owner
      LocalizedCacheTopology topology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      Map<Address, IntSet> backups = backupOwnersOfSegments(topology, extractCommandSegments(command, topology));
      if (backups.isEmpty()) {
         return rv;
      }
      boolean isSync = isSynchronous(command);
      CompletableFuture[] futures = isSync ? new CompletableFuture[backups.size()] : null;
      int future = 0;
      for (Entry<Address, IntSet> backup : backups.entrySet()) {
         C copy = helper.copyForBackup(command, topology, backup.getKey(), backup.getValue());
         copy.setTopologyId(command.getTopologyId());
         Address backupOwner = backup.getKey();
         if (isSync) {
            futures[future++] = rpcManager
                  .invokeCommand(backupOwner, copy, SingleResponseCollector.validOnly(),
                        rpcManager.getSyncRpcOptions())
                  .toCompletableFuture();
         } else {
            rpcManager.sendTo(backupOwner, copy, DeliverOrder.PER_SENDER);
         }
      }
      return isSync ? asyncValue(CompletableFuture.allOf(futures).thenApply(nil -> rv)) : rv;

   }

   private <C extends WriteCommand> IntSet extractCommandSegments(C command, LocalizedCacheTopology topology) {
      IntSet keySegments = IntSets.mutableEmptySet(topology.getNumSegments());
      for (Object key : command.getAffectedKeys()) {
         keySegments.set(keyPartitioner.getSegment(key));
      }
      return keySegments;
   }

   private <C extends WriteCommand> InvocationSuccessFunction createRemoteCallback(WriteManyCommandHelper<C, ?, ?> helper) {
      return (ctx, command, rv) -> writeManyRemoteCallback(helper, ctx, (C) command, rv);
   }
}
