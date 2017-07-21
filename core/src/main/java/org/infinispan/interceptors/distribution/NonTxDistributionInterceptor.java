package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
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
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.concurrent.CompletableFutures;
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

   private Map<Address, Set<Integer>> primaryOwnersOfSegments(ConsistentHash ch) {
      Map<Address, Set<Integer>> map = new HashMap<>(ch.getMembers().size());
      for (int segment = 0; segment < ch.getNumSegments(); ++segment) {
         Address owner = ch.locatePrimaryOwnerForSegment(segment);
         map.computeIfAbsent(owner, o -> new HashSet<>()).add(segment);
      }
      return map;
   }

   // we're assuming that this function is ran on primary owner of given segments
   private Map<Address, Set<Integer>> backupOwnersOfSegments(ConsistentHash ch, Set<Integer> segments) {
      Map<Address, Set<Integer>> map = new HashMap<>(ch.getMembers().size());
      if (ch.isReplicated()) {
         for (Address member : ch.getMembers()) {
            map.put(member, segments);
         }
         map.remove(rpcManager.getAddress());
      } else {
         for (Integer segment : segments) {
            List<Address> owners = ch.locateOwnersForSegment(segment);
            for (int i = 1; i < owners.size(); ++i) {
               map.computeIfAbsent(owners.get(i), o -> new HashSet<>()).add(segment);
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
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, Set<Integer>> segmentMap = primaryOwnersOfSegments(ch);
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(segmentMap.size());

         // Go through all members, for this node invokeNext (if this node is an owner of some keys),
         // for the others (that own some keys) issue a remote call.
         // Everything is finished when allFuture is completed
         for (Entry<Address, Set<Integer>> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            Set<Integer> segments = pair.getValue();
            handleSegmentsForWriteOnlyManyCommand(ctx, command, helper, ch, allFuture, member, segments);
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         // check that we have all the data we need
         return handleRemoteWriteOnlyManyCommand(ctx, command, helper);
      }
   }

   private <C extends WriteCommand, Container, Item> void handleSegmentsForWriteOnlyManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper, ConsistentHash ch,
         CountDownCompletableFuture allFuture, Address member, Set<Integer> segments) {
      if (member.equals(rpcManager.getAddress())) {
         Container myItems = filterAndWrap(ctx, command, segments, helper);

         C localCommand = helper.copyForLocal(command, myItems);
         // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
         // calls complete.
         invokeNextAndFinally(ctx, localCommand,
                              createLocalInvocationHandler(ch, allFuture, segments, helper, (f, rv) -> {
                          }));
         return;
      }

      C copy = helper.copyForPrimary(command, ch, segments);
      int size = helper.getItems(copy).size();
      if (size <= 0) {
         allFuture.countDown();
         return;
      }

      rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, defaultSyncOptions)
            .whenComplete((responseMap, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               } else {
                  if (getSuccessfulResponseOrFail(responseMap, allFuture,
                        rsp -> allFuture.completeExceptionally(OutdatedTopologyException.INSTANCE)) == null) {
                     return;
                  }
                  allFuture.countDown();
               }
            });
   }

   private <C extends WriteCommand, Item> Object handleRemoteWriteOnlyManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, ?, Item> helper) {
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         }
      }

      if (helper.shouldRegisterRemoteCallback(command)) {
         return invokeNextThenApply(ctx, command, helper.remoteCallback);
      } else {
         return invokeNext(ctx, command);
      }
   }

   private <C extends WriteCommand, Container, Item> Container filterAndWrap(
         InvocationContext ctx, C command, Set<Integer> segments,
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

   private <C extends WriteCommand, Container, Item> Object handleReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Item, Container> helper) throws Exception {
      // TODO: due to possible repeating of the operation (after OutdatedTopologyException is thrown)
      // it is possible that the function will be applied multiple times on some of the nodes.
      // There is no general solution for this ATM; proper solution will probably record CommandInvocationId
      // in the entry, and implement some housekeeping
      ConsistentHash ch = checkTopologyId(command).getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, Set<Integer>> segmentMap = primaryOwnersOfSegments(ch);
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
         for (Entry<Address, Set<Integer>> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            Set<Integer> segments = pair.getValue();
            if (member.equals(rpcManager.getAddress())) {
               handleLocalSegmentsForReadWriteManyCommand(ctx, command, helper, ch, allFuture, offset, segments);
            } else {
               handleRemoteSegmentsForReadWriteManyCommand(command, helper, ch, allFuture, offset, member, segments);
            }
         }
         return asyncValue(allFuture);
      } else { // origin is not local
         return handleRemoteReadWriteManyCommand(ctx, command, helper);
      }
   }

   private <C extends WriteCommand, Container, Item> void handleLocalSegmentsForReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper, ConsistentHash ch,
         MergingCompletableFuture<Object> allFuture, MutableInt offset, Set<Integer> segments) throws Exception {
      Container myItems = helper.newContainer();
      List<CompletableFuture<?>> retrievals = null;
      // Filter command keys/entries into the collection, and record remote retrieval for those that are not
      // in the context yet
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         if (segments.contains(keyPartitioner.getSegment(key))) {
            helper.accumulate(myItems, item);
            retrievals = addRemoteGet(ctx, command, retrievals, key);
         }
      }
      int size = helper.containerSize(myItems);
      if (size == 0) {
         allFuture.countDown();
         return;
      }
      final int myOffset = offset.value;
      offset.value += size;

      C localCommand = helper.copyForLocal(command, myItems);
      InvocationFinallyAction handler =
            createLocalInvocationHandler(ch, allFuture, segments, helper, MergingCompletableFuture.moveListItemsToFuture(myOffset));
      if (retrievals == null) {
         invokeNextAndFinally(ctx, localCommand, handler);
      } else {
         // We must wait until all retrievals finish before proceeding with the local command
         CompletableFuture[] ra = retrievals.toArray(new CompletableFuture[retrievals.size()]);
         Object result = asyncInvokeNext(ctx, command, CompletableFuture.allOf(ra));
         makeStage(result).andFinally(ctx, command, handler);
      }
      // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
      // calls complete.
   }

   private <C extends WriteCommand, Item> void handleRemoteSegmentsForReadWriteManyCommand(
         C command, WriteManyCommandHelper<C, ?, Item> helper,
         ConsistentHash ch, MergingCompletableFuture<Object> allFuture,
         MutableInt offset, Address member, Set<Integer> segments) {
      final int myOffset = offset.value;
      // TODO: here we iterate through all entries - is the ReadOnlySegmentAwareMap really worth it?
      C copy = helper.copyForPrimary(command, ch, segments);
      int size = helper.getItems(copy).size();
      offset.value += size;
      if (size <= 0) {
         allFuture.countDown();
         return;
      }

      // Send the command to primary owner
      rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, defaultSyncOptions)
            .whenComplete((responses, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               } else {
                  SuccessfulResponse response = getSuccessfulResponseOrFail(responses, allFuture,
                        rsp -> allFuture.completeExceptionally(OutdatedTopologyException.INSTANCE));
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
      List<CompletableFuture<?>> retrievals = null;
      // check that we have all the data we need
      for (Item item : helper.getItems(command)) {
         retrievals = addRemoteGet(ctx, command, retrievals, helper.item2key(item));
      }

      CompletableFuture<Void> delay;
      if (retrievals != null) {
         CompletableFuture[] ra = retrievals.toArray(new CompletableFuture[retrievals.size()]);
         delay = CompletableFuture.allOf(ra);
      } else {
         delay = CompletableFutures.completedNull();
      }
      Object result = asyncInvokeNext(ctx, command, delay);
      if (helper.shouldRegisterRemoteCallback(command)) {
         return makeStage(result).thenApply(ctx, command, helper.remoteCallback);
      } else {
         return result;
      }
   }

   private List<CompletableFuture<?>> addRemoteGet(InvocationContext ctx, WriteCommand command,
                                                   List<CompletableFuture<?>> retrievals, Object key) throws Exception {
      CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (cacheEntry == null) {
         // this should be a rare situation, so we don't mind being a bit ineffective with the remote gets
         if (command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP) || command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL)) {
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         } else {
            if (retrievals == null) {
               retrievals = new ArrayList<>();
            }
            GetCacheEntryCommand fakeGetCommand = cf.buildGetCacheEntryCommand(key, command.getFlagsBitSet());
            CompletableFuture<?> getFuture = remoteGet(ctx, fakeGetCommand, fakeGetCommand.getKey(), true);
            retrievals.add(getFuture);
         }
      }
      return retrievals;
   }

   private <C extends WriteCommand, F extends CountDownCompletableFuture, Item>
   InvocationFinallyAction createLocalInvocationHandler(
         ConsistentHash ch, F allFuture, Set<Integer> segments, WriteManyCommandHelper<C, ?, Item> helper,
         BiConsumer<F, Object> returnValueConsumer) {
      return (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            returnValueConsumer.accept(allFuture, rv);

            Map<Address, Set<Integer>> backupOwners = backupOwnersOfSegments(ch, segments);
            for (Entry<Address, Set<Integer>> backup : backupOwners.entrySet()) {
               // rCommand is the original command
               C backupCopy = helper.copyForBackup((C) rCommand, ch, backup.getValue());
               if (helper.getItems(backupCopy).isEmpty()) continue;
               Set<Address> backupOwner = Collections.singleton(backup.getKey());
               if (isSynchronous(backupCopy)) {
                  allFuture.increment();
                  rpcManager.invokeRemotelyAsync(backupOwner, backupCopy, defaultSyncOptions)
                        .whenComplete((responseMap, remoteThrowable) -> {
                           if (remoteThrowable != null) {
                              allFuture.completeExceptionally(remoteThrowable);
                           } else {
                              allFuture.countDown();
                           }
                        });
               } else {
                  rpcManager.invokeRemotelyAsync(backupOwner, backupCopy, defaultAsyncOptions);
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
      ConsistentHash ch = checkTopologyId(command).getWriteConsistentHash();
      // We have already checked that the command topology is actual, so we can assume that we really are primary owner
      Map<Address, Set<Integer>> backups = backupOwnersOfSegments(ch, ch.getPrimarySegmentsForOwner(rpcManager.getAddress()));
      if (backups.isEmpty()) {
         return rv;
      }
      boolean isSync = isSynchronous(command);
      CompletableFuture[] futures = isSync ? new CompletableFuture[backups.size()] : null;
      int future = 0;
      for (Entry<Address, Set<Integer>> backup : backups.entrySet()) {
         C copy = helper.copyForBackup(command, ch, backup.getValue());
         if (isSync) {
            futures[future++] = rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultSyncOptions);
         } else {
            rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultAsyncOptions);
         }
      }
      return isSync ? asyncValue(CompletableFuture.allOf(futures).thenApply(nil -> rv)) : rv;
   }

   private <C extends WriteCommand> InvocationSuccessFunction createRemoteCallback(WriteManyCommandHelper<C, ?, ?> helper) {
      return (ctx, command, rv) -> writeManyRemoteCallback(helper, ctx, (C) command, rv);
   }
}
