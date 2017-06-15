package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.infinispan.commands.VisitableCommand;
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
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
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

   private final PutMapHelper putMapHelper = new PutMapHelper();
   private final ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper();
   private final ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper();
   private final WriteOnlyManyEntriesHelper writeOnlyManyEntriesHelper = new WriteOnlyManyEntriesHelper();
   private final WriteOnlyManyHelper writeOnlyManyHelper = new WriteOnlyManyHelper();

   private static BiConsumer<MergingCompletableFuture<Object>, Object> moveListItemsToFuture(int myOffset) {
      return (f, rv) -> {
         Collection<?> items;
         if (rv == null && f.results == null) {
            return;
         } else if (rv instanceof Map) {
            items = ((Map) rv).entrySet();
         } else if (rv instanceof Collection) {
            items = (Collection<?>) rv;
         } else {
            f.completeExceptionally(new IllegalArgumentException("Unexpected result value " + rv));
            return;
         }
         if (trace) {
            log.tracef("Copying %d items %s to results (%s), starting offset %d", items.size(), items,
                  Arrays.toString(f.results), myOffset);
         }
         Iterator<?> it = items.iterator();
         for (int i = 0; it.hasNext(); ++i) {
            f.results[myOffset + i] = it.next();
         }
      };
   }

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
      } else if (ch.getNumOwners() > 1) {
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
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(ctx, segmentMap.size());

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
               } else try {
                  // ignore actual response value
                  getSingleSuccessfulResponseOrFail(responseMap, allFuture);
                  allFuture.countDown();
               } catch (Throwable t) {
                  allFuture.completeExceptionally(t);
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

      if (helper.shouldRegisterRemoteCallback(command, null)) {
         return invokeNextThenApply(ctx, command, helper);
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
               = new MergingCompletableFuture<>(ctx, segmentMap.size(), results, helper::transformResult);
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
         return handleRemoteReadWriteManyCommand(ctx, command, helper, ch);
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
            createLocalInvocationHandler(ch, allFuture, segments, helper, moveListItemsToFuture(myOffset));
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
                  Response response = getSingleSuccessfulResponseOrFail(responses, allFuture);
                  if (response == null) return;
                  Object responseValue = ((SuccessfulResponse) response).getResponseValue();
                  moveListItemsToFuture(myOffset).accept(allFuture, responseValue);
                  allFuture.countDown();
               }
            });
   }

   private <C extends WriteCommand, Item> Object handleRemoteReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, ?, Item> helper, ConsistentHash ch) throws Exception {
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
      if (helper.shouldRegisterRemoteCallback(command, ch)) {
         return makeStage(result).thenApply(ctx, command, helper);
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

   private abstract class WriteManyCommandHelper<C extends WriteCommand, Container, Item>
         implements InvocationSuccessFunction {
      public abstract C copyForLocal(C cmd, Container container);

      public abstract C copyForPrimary(C cmd, ConsistentHash ch, Set<Integer> segments);

      public abstract C copyForBackup(C cmd, ConsistentHash ch, Set<Integer> segments);

      public abstract Collection<Item> getItems(C cmd);

      public abstract Object item2key(Item item);

      public abstract Container newContainer();

      public abstract void accumulate(Container container, Item item);

      public abstract int containerSize(Container container);

      public abstract boolean shouldRegisterRemoteCallback(C cmd, ConsistentHash ch);

      public abstract Object transformResult(Object[] results);

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         C original = (C) rCommand;
         ConsistentHash ch = checkTopologyId(original).getWriteConsistentHash();
         // We have already checked that the command topology is actual, so we can assume that we really are primary owner
         Map<Address, Set<Integer>> backups = backupOwnersOfSegments(ch, ch.getPrimarySegmentsForOwner(rpcManager.getAddress()));
         if (backups.isEmpty()) {
            return rv;
         }
         boolean isSync = isSynchronous(original);
         CompletableFuture[] futures = isSync ? new CompletableFuture[backups.size()] : null;
         int future = 0;
         for (Entry<Address, Set<Integer>> backup : backups.entrySet()) {
            C copy = copyForBackup(original, ch, backup.getValue());
            if (isSync) {
               futures[future++] = rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultSyncOptions);
            } else {
               rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultAsyncOptions);
            }
         }
         return isSync ? asyncValue(CompletableFuture.allOf(futures).thenApply(nil -> rv)) : rv;
      }
   }

   private class PutMapHelper extends WriteManyCommandHelper<PutMapCommand, Map<Object, Object>, Entry<Object, Object>> {
      @Override
      public PutMapCommand copyForLocal(PutMapCommand cmd, Map<Object, Object> container) {
         return new PutMapCommand(cmd).withMap(container);
      }

      @Override
      public PutMapCommand copyForPrimary(PutMapCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         return new PutMapCommand(cmd).withMap(new ReadOnlySegmentAwareMap<>(cmd.getMap(), ch, segments));
      }

      @Override
      public PutMapCommand copyForBackup(PutMapCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         PutMapCommand copy = new PutMapCommand(cmd).withMap(new ReadOnlySegmentAwareMap(cmd.getMap(), ch, segments));
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Entry<Object, Object>> getItems(PutMapCommand cmd) {
         return cmd.getMap().entrySet();
      }

      @Override
      public Object item2key(Entry<Object, Object> entry) {
         return entry.getKey();
      }

      @Override
      public Map<Object, Object> newContainer() {
         return new HashMap<>();
      }

      @Override
      public void accumulate(Map<Object, Object> map, Entry<Object, Object> entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean shouldRegisterRemoteCallback(PutMapCommand cmd, ConsistentHash ch) {
         return !(cmd.isForwarded() || ch.getNumOwners() <= 1);
      }

      @Override
      public Object transformResult(Object[] results) {
         if (results == null) return null;
         Map<Object, Object> result = new HashMap<>();
         for (Object r : results) {
            Map.Entry<Object, Object> entry = (Entry<Object, Object>) r;
            result.put(entry.getKey(), entry.getValue());
         }
         return result;
      }
   }

   private class ReadWriteManyEntriesHelper extends WriteManyCommandHelper<ReadWriteManyEntriesCommand, Map<Object, Object>, Entry<Object, Object>> {
      @Override
      public ReadWriteManyEntriesCommand copyForLocal(ReadWriteManyEntriesCommand cmd, Map<Object, Object> entries) {
         return new ReadWriteManyEntriesCommand(cmd).withEntries(entries);
      }

      @Override
      public ReadWriteManyEntriesCommand copyForPrimary(ReadWriteManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         return new ReadWriteManyEntriesCommand(cmd)
               .withEntries(new ReadOnlySegmentAwareMap<>(cmd.getEntries(), ch, segments));
      }

      @Override
      public ReadWriteManyEntriesCommand copyForBackup(ReadWriteManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(cmd)
               .withEntries(new ReadOnlySegmentAwareMap(cmd.getEntries(), ch, segments));
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Entry<Object, Object>> getItems(ReadWriteManyEntriesCommand cmd) {
         return cmd.getEntries().entrySet();
      }

      @Override
      public Object item2key(Entry<Object, Object> entry) {
         return entry.getKey();
      }

      @Override
      public Map<Object, Object> newContainer() {
         return new HashMap<>();
      }

      @Override
      public void accumulate(Map<Object, Object> map, Entry<Object, Object> entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean shouldRegisterRemoteCallback(ReadWriteManyEntriesCommand cmd, ConsistentHash ch) {
         return !(cmd.isForwarded() || ch.getNumOwners() <= 1);
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }
   }

   private class ReadWriteManyHelper extends WriteManyCommandHelper<ReadWriteManyCommand, Collection<Object>, Object> {
      @Override
      public ReadWriteManyCommand copyForLocal(ReadWriteManyCommand cmd, Collection<Object> keys) {
         return new ReadWriteManyCommand(cmd).withKeys(keys);
      }

      @Override
      public ReadWriteManyCommand copyForPrimary(ReadWriteManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         return new ReadWriteManyCommand(cmd).withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      }

      @Override
      public ReadWriteManyCommand copyForBackup(ReadWriteManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         ReadWriteManyCommand copy = new ReadWriteManyCommand(cmd).withKeys(
               new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Object> getItems(ReadWriteManyCommand cmd) {
         return cmd.getAffectedKeys();
      }

      @Override
      public Object item2key(Object key) {
         return key;
      }

      @Override
      public Collection<Object> newContainer() {
         return new ArrayList<>();
      }

      @Override
      public void accumulate(Collection<Object> list, Object key) {
         list.add(key);
      }

      @Override
      public int containerSize(Collection<Object> list) {
         return list.size();
      }

      @Override
      public boolean shouldRegisterRemoteCallback(ReadWriteManyCommand cmd, ConsistentHash ch) {
         return !(cmd.isForwarded() || ch.getNumOwners() <= 1);
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }
   }

   private class WriteOnlyManyEntriesHelper extends WriteManyCommandHelper<WriteOnlyManyEntriesCommand, Map<Object, Object>, Entry<Object, Object>> {

      @Override
      public WriteOnlyManyEntriesCommand copyForLocal(WriteOnlyManyEntriesCommand cmd, Map<Object, Object> entries) {
         return new WriteOnlyManyEntriesCommand(cmd).withEntries(entries);
      }

      @Override
      public WriteOnlyManyEntriesCommand copyForPrimary(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         return new WriteOnlyManyEntriesCommand(cmd)
               .withEntries(new ReadOnlySegmentAwareMap<>(cmd.getEntries(), ch, segments));
      }

      @Override
      public WriteOnlyManyEntriesCommand copyForBackup(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(cmd)
               .withEntries(new ReadOnlySegmentAwareMap(cmd.getEntries(), ch, segments));
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Entry<Object, Object>> getItems(WriteOnlyManyEntriesCommand cmd) {
         return cmd.getEntries().entrySet();
      }

      @Override
      public Object item2key(Entry<Object, Object> entry) {
         return entry.getKey();
      }

      @Override
      public Map<Object, Object> newContainer() {
         return new HashMap<>();
      }

      @Override
      public void accumulate(Map<Object, Object> map, Entry<Object, Object> entry) {
         map.put(entry.getKey(), entry.getValue());
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean shouldRegisterRemoteCallback(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch) {
         return !cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }
   }

   private class WriteOnlyManyHelper extends WriteManyCommandHelper<WriteOnlyManyCommand, Collection<Object>, Object> {
      @Override
      public WriteOnlyManyCommand copyForLocal(WriteOnlyManyCommand cmd, Collection<Object> keys) {
         return new WriteOnlyManyCommand(cmd).withKeys(keys);
      }

      @Override
      public WriteOnlyManyCommand copyForPrimary(WriteOnlyManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         return new WriteOnlyManyCommand(cmd)
               .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      }

      @Override
      public WriteOnlyManyCommand copyForBackup(WriteOnlyManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
         WriteOnlyManyCommand copy = new WriteOnlyManyCommand(cmd)
               .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
         copy.setForwarded(true);
         return copy;
      }

      @Override
      public Collection<Object> getItems(WriteOnlyManyCommand cmd) {
         return cmd.getAffectedKeys();
      }

      @Override
      public Object item2key(Object key) {
         return key;
      }

      @Override
      public Collection<Object> newContainer() {
         return new ArrayList<>();
      }

      @Override
      public void accumulate(Collection<Object> list, Object key) {
         list.add(key);
      }

      @Override
      public int containerSize(Collection<Object> list) {
         return list.size();
      }

      @Override
      public boolean shouldRegisterRemoteCallback(WriteOnlyManyCommand cmd, ConsistentHash ch) {
         return !cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }
   }
}
