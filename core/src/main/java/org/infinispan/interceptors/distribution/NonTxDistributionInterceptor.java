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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.StrictOrderingCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
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
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
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

//   private Map<Address, Set<Integer>> primaryOwnersOfSegments(ConsistentHash ch) {
//      Map<Address, Set<Integer>> map = new HashMap<>(ch.getMembers().size());
//      for (int segment = 0; segment < ch.getNumSegments(); ++segment) {
//         Address owner = ch.locatePrimaryOwnerForSegment(segment);
//         map.computeIfAbsent(owner, o -> new HashSet<>()).add(segment);
//      }
//      return map;
//   }

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
      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
      if (ctx.isOriginLocal()) {
         Collection<Item> items = helper.getItems(command);
         if (items.isEmpty()) {
            return null;
         }
         PrimaryOwnerClassifier<Container, Item> filter = new PrimaryOwnerClassifier<>(cacheTopology, items.size(), helper);
         items.forEach(filter::add);
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(ctx, filter.primaries.size());

         Container localItems = filter.primaries.remove(rpcManager.getAddress());
         if (localItems != null) {
            C localCommand = helper.copyForPrimary(command, localItems);
            handleLocalEntriesForWriteOnlyManyCommand(ctx, localCommand, helper, cacheTopology, helper.asCollection(localItems), allFuture);
         }

         for (Entry<Address, Container> remote : filter.primaries.entrySet()) {
            C copy = helper.copyForPrimary(command, remote.getValue());
            rpcManager.invokeRemotelyAsync(Collections.singletonList(remote.getKey()), copy, defaultSyncOptions)
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
            CountDownCompletableFuture allFuture = new CountDownCompletableFuture(ctx, 1);
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
            .collect(Collectors.toMap(Map.Entry::getKey, e -> helper.copyForBackup(localCommand, e.getValue())));

      localCommand.setAuthoritative(true);
      InvocationFinallyAction handler = createLocalInvocationHandler(backupCommands, null, allFuture, helper, (f, r) -> {});
      invokeNextAndFinally(ctx, localCommand, handler);
   }

   private <C extends WriteCommand, Container, Item> Object handleReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper) throws Exception {
      LocalizedCacheTopology cacheTopology = checkTopologyId(command.getTopologyId());
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
         MergingCompletableFuture<Object> allFuture
               = new MergingCompletableFuture<>(ctx, filter.primaries.size(), results, helper::transformResult);
         MutableInt offset = new MutableInt();

         Container localItems = filter.primaries.remove(rpcManager.getAddress());
         if (localItems != null) {
            C localCommand = helper.copyForPrimary(command, localItems);
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
                  = new MergingCompletableFuture<>(ctx, 1, results, helper::transformResult);
            handleLocalEntriesForReadWriteManyCommand(ctx, command, command, helper, cacheTopology,
                  helper.getItems(command), allFuture, new MutableInt());
            return asyncValue(allFuture);
         }
      }
   }

   private <C extends WriteCommand, Container, Item> void handleLocalEntriesForReadWriteManyCommand(
         InvocationContext ctx, C command, C localCommand, WriteManyCommandHelper<C, Container, Item> helper,
         LocalizedCacheTopology cacheTopology, Collection<Item> localItems,
         MergingCompletableFuture<Object> allFuture, MutableInt offset) throws Exception {
      Map<Address, Container> backupItems = new HashMap<>();
      Map<Object, Object> completedResults = null;

      // TODO: optimize lambdas
      Consumer<Item> addToBackup = item -> {
         Object key = helper.item2key(item);
         DistributionInfo info = cacheTopology.getDistribution(key);
         for (Address backup : info.writeBackups()) {
            helper.accumulate(backupItems.computeIfAbsent(backup, b -> helper.newContainer()), item);
         }
      };
      Map<Address, Collection<Object>> fullBackupKeys = null;
      if (localCommand.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
         Map<Address, Collection<Object>> fbk = fullBackupKeys = new HashMap<>();
         completedResults = checkInvocationRecords(ctx, command, localItems, addToBackup, item -> {
            Object key = helper.item2key(item);
            DistributionInfo info = cacheTopology.getDistribution(key);
            for (Address backup : info.writeBackups()) {
               fbk.computeIfAbsent(backup, b -> new HashSet<>()).add(key);
            }
         }, helper);
      } else {
         localItems.forEach(addToBackup);
      }
      Map<Address, C> backupCommands = backupItems.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> {
         C backupCommand = helper.copyForBackup(command, e.getValue());
         if (backupCommand instanceof StrictOrderingCommand) {
            StrictOrderingCommand soc = (StrictOrderingCommand) backupCommand;
            for (Item item : helper.asCollection(e.getValue())) {
               Object key = helper.item2key(item);
               ctx.lookupEntry(key).metadata().flatMap(Metadata::lastInvocationOpt)
                     .map(InvocationRecord::getId).ifPresent(id -> soc.setLastInvocationId(key, id));
            }
         }
         return backupCommand;
      }));

      int size = localItems.size();
      if (size == 0) {
         allFuture.countDown();
         return;
      }
      if (completedResults != null) {
         for (Entry<Object, Object> entry : completedResults.entrySet()) {
            allFuture.results[offset.value++] = helper.transformResult(entry.getKey(), entry.getValue());
         }
      }
      final int myOffset = offset.value;
      offset.value += size;

      localCommand.setAuthoritative(true);
      InvocationFinallyAction handler = createLocalInvocationHandler(backupCommands, fullBackupKeys, allFuture, helper, moveListItemsToFuture(myOffset));
      invokeNextAndFinally(ctx, localCommand, handler);
      // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
      // calls complete.
   }

   private <C extends WriteCommand, Container, Item> void forwardReadWriteManyToPrimary(
         C command, WriteManyCommandHelper<C, Container, Item> helper,
         Container items, MergingCompletableFuture<Object> allFuture,
         MutableInt offset, Address member) {
      final int myOffset = offset.value;
      C copy = helper.copyForPrimary(command, items);
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

   protected <C extends WriteCommand, Container, Item> Object handleBackupForReadWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper, LocalizedCacheTopology cacheTopology) throws Exception {
      Map<Object, Object> completedResults = null;
      CountDownCompletableFuture countDown = null;
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         CacheEntry entry = ctx.lookupEntry(key);
         if (command instanceof StrictOrderingCommand) {
            StrictOrderingCommand soc = (StrictOrderingCommand) command;
            if (soc.isStrictOrdering()) {
               CommandInvocationId lastInvocationId = soc.getLastInvocationId(key);
               CommandInvocationId entryInvocationId = Optional.ofNullable(entry)
                     .flatMap(CacheEntry::metadata).flatMap(Metadata::lastInvocationOpt)
                     .map(InvocationRecord::getId).orElse(null);
               if (entry != null && Objects.equals(lastInvocationId, entryInvocationId)) {
                  // We can apply the write, because it's based on the same previous value
                  // Note: previous invocation id might expire on primary, therefore if it sends a command without
                  // valid last invocation id and we are not read owner, we cannot apply the value because primary
                  // *could* have a value (without any id)
               } else {
                  if (completedResults == null) {
                     completedResults = new HashMap<>();
                     countDown = new CountDownCompletableFuture(ctx, 1);
                  } else {
                     countDown.increment();
                  }
                  DistributionInfo info = cacheTopology.getDistribution(key);
                  ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(key, FlagBitSets.WITH_INVOCATION_RECORDS);
                  clusteredGetCommand.setTopologyId(command.getTopologyId());

                  Map<Object, Object> cr = completedResults;
                  CountDownCompletableFuture cd = countDown;
                  rpcManager.invokeRemotelyAsync(info.primaryAsList(), clusteredGetCommand, defaultSyncOptions).thenApply(
                        responses -> handleStrictOrderingResponse(responses, ctx, command, key, info, lastInvocationId, r -> {
                           synchronized (cr) {
                              cr.put(key, r);
                           }
                           cd.countDown();
                        })
                  );
               }
            }
         }
         if (entry == null) {
            // Retrieving remote value may be impossible as it might be already overwritten on all owners.
            assert command.loadType() != VisitableCommand.LoadType.OWNER;
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
         } else if (command.hasAnyFlag(FlagBitSets.COMMAND_RETRY)) {
            Metadata metadata = entry.getMetadata();
            if (metadata != null) {
               InvocationRecord invocationRecord = metadata.invocation(command.getCommandInvocationId());
               if (invocationRecord != null) {
                  command.setCompleted(key);
                  if (completedResults == null) {
                     completedResults = new HashMap<>();
                  }
                  synchronized (completedResults) {
                     completedResults.put(key, invocationRecord.returnValue);
                  }
                  cdl.notifyCommitEntry(invocationRecord.isCreated(), invocationRecord.isRemoved(), false,
                        entry, ctx, command, null, null);
               }
            }
         }
      }

      if (completedResults == null) {
         command.setAuthoritative(false);
         return invokeNext(ctx, command);
      } else {
         Container container = helper.newContainer();
         for (Item item : helper.getItems(command)) {
            Object key = helper.item2key(item);
            if (!completedResults.containsKey(key)) {
               helper.accumulate(container, item);
            }
         }
         C copy = helper.copyForBackup(command, container);
         copy.setAuthoritative(false);
         Map<Object, Object> cr = completedResults;
         InvocationSuccessFunction mergeResults = (rCtx, rCommand, rv) -> helper.mergeResults(rv, cr);
         if (countDown == null) {
            return invokeNextThenApply(ctx, copy, mergeResults);
         } else {
            return makeStage(asyncInvokeNext(ctx, copy, countDown)).thenApply(ctx, copy, mergeResults);
         }
      }
   }

   private <C extends WriteCommand, F extends CountDownCompletableFuture, Container, Item>
   InvocationFinallyAction createLocalInvocationHandler(
         Map<Address, C> backupCommands, Map<Address, Collection<Object>> fullBackupKeys,
         F allFuture, WriteManyCommandHelper<C, Container, Item> helper,
         BiConsumer<F, Object> returnValueConsumer) {
      return (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            returnValueConsumer.accept(allFuture, rv);

            C command = (C) rCommand;
            for (Entry<Address, C> backup : backupCommands.entrySet()) {
               // rCommand is the original command
               C backupCopy = backup.getValue();
               Set<Address> backupOwner = Collections.singleton(backup.getKey());
               if (isSynchronous(backupCopy)) {
                  allFuture.increment();
                  rpcManager.invokeRemotelyAsync(backupOwner, backupCopy, defaultSyncOptions).whenComplete(allFuture);
               } else {
                  rpcManager.invokeRemotelyAsync(backupOwner, backupCopy, defaultAsyncOptions);
               }
            }
            if (fullBackupKeys != null) {
               for (Entry<Address, Collection<Object>> fullBackup : fullBackupKeys.entrySet()) {
                  Set<Address> backupOwner = Collections.singleton(fullBackup.getKey());
                  for (Object key : fullBackup.getValue()) {
                     CacheEntry entry = rCtx.lookupEntry(key);
                     // provided result should be in the metadata in context
                     PutKeyValueCommand fullBackupCommand = cf.buildPutKeyValueCommand(key, entry.getValue(),
                           entry.getMetadata(), (command.getFlagsBitSet() & ~FlagBitSets.DELTA_WRITE) | FlagBitSets.WITH_INVOCATION_RECORDS);
                     if (isSynchronous(command)) {
                        allFuture.increment();
                        rpcManager.invokeRemotelyAsync(backupOwner, fullBackupCommand, defaultSyncOptions).whenComplete(allFuture);
                     } else {
                        rpcManager.invokeRemotelyAsync(backupOwner, fullBackupCommand, defaultAsyncOptions);
                     }
                  }
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

   private class ReadWriteManyEntriesHelper extends WriteManyCommandHelper<ReadWriteManyEntriesCommand, Map<Object, Object>, Entry<Object, Object>> {
      @Override
      public ReadWriteManyEntriesCommand copyForPrimary(ReadWriteManyEntriesCommand cmd, Map<Object, Object> items) {
         return new ReadWriteManyEntriesCommand(cmd).withEntries(items);
      }

      @Override
      public ReadWriteManyEntriesCommand copyForBackup(ReadWriteManyEntriesCommand cmd, Map<Object, Object> items) {
         ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(cmd).withEntries(items);
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
      public Collection<Entry<Object, Object>> asCollection(Map<Object, Object> items) {
         return items.entrySet();
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean isForwarded(ReadWriteManyEntriesCommand cmd) {
         return cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }

      @Override
      public Object transformResult(Object key, Object result) {
         return result;
      }

      @Override
      public Object mergeResults(Object rv, Map<Object, Object> results) {
         ((List) rv).addAll(results.values());
         return rv;
      }
   }

   private class ReadWriteManyHelper extends WriteManyCommandHelper<ReadWriteManyCommand, Collection<Object>, Object> {
      @Override
      public ReadWriteManyCommand copyForPrimary(ReadWriteManyCommand cmd, Collection<Object> items) {
         return new ReadWriteManyCommand(cmd).withKeys(items);
      }

      @Override
      public ReadWriteManyCommand copyForBackup(ReadWriteManyCommand cmd, Collection<Object> items) {
         ReadWriteManyCommand copy = new ReadWriteManyCommand(cmd).withKeys(items);
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
      public Collection<Object> asCollection(Collection<Object> items) {
         return items;
      }

      @Override
      public int containerSize(Collection<Object> list) {
         return list.size();
      }

      @Override
      public boolean isForwarded(ReadWriteManyCommand cmd) {
         return cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }

      @Override
      public Object transformResult(Object key, Object result) {
         return result;
      }

      @Override
      public Object mergeResults(Object rv, Map<Object, Object> results) {
         ((List) rv).addAll(results.values());
         return rv;
      }
   }

   private class WriteOnlyManyEntriesHelper extends WriteManyCommandHelper<WriteOnlyManyEntriesCommand, Map<Object, Object>, Entry<Object, Object>> {
      @Override
      public WriteOnlyManyEntriesCommand copyForPrimary(WriteOnlyManyEntriesCommand cmd, Map<Object, Object> items) {
         return new WriteOnlyManyEntriesCommand(cmd).withEntries(items);
      }

      @Override
      public WriteOnlyManyEntriesCommand copyForBackup(WriteOnlyManyEntriesCommand cmd, Map<Object, Object> items) {
         WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(cmd).withEntries(items);
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
      public Collection<Entry<Object, Object>> asCollection(Map<Object, Object> items) {
         return items.entrySet();
      }

      @Override
      public int containerSize(Map<Object, Object> map) {
         return map.size();
      }

      @Override
      public boolean isForwarded(WriteOnlyManyEntriesCommand cmd) {
         return cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }

      @Override
      public Object transformResult(Object key, Object result) {
         return result;
      }

      @Override
      public Object mergeResults(Object rv, Map<Object, Object> results) {
         assert rv == null;
         return null;
      }
   }

   private class WriteOnlyManyHelper extends WriteManyCommandHelper<WriteOnlyManyCommand, Collection<Object>, Object> {
      @Override
      public WriteOnlyManyCommand copyForPrimary(WriteOnlyManyCommand cmd, Collection<Object> items) {
         return new WriteOnlyManyCommand(cmd).withKeys(items);
      }

      @Override
      public WriteOnlyManyCommand copyForBackup(WriteOnlyManyCommand cmd, Collection<Object> items) {
         WriteOnlyManyCommand copy = new WriteOnlyManyCommand(cmd).withKeys(items);
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
      public Collection<Object> asCollection(Collection<Object> items) {
         return items;
      }

      @Override
      public int containerSize(Collection<Object> list) {
         return list.size();
      }

      @Override
      public boolean isForwarded(WriteOnlyManyCommand cmd) {
         return cmd.isForwarded();
      }

      @Override
      public Object transformResult(Object[] results) {
         return results == null ? null : Arrays.asList(results);
      }

      @Override
      public Object transformResult(Object key, Object result) {
         return result;
      }

      @Override
      public Object mergeResults(Object rv, Map<Object, Object> results) {
         assert rv == null;
         return null;
      }
   }
}
