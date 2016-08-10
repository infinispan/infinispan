package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationComposeSuccessHandler;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
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

   private final InvocationComposeSuccessHandler readWriteManyPrimaryHandler
         = primaryReturnHandler(NonTxDistributionInterceptor::readWriteManyCommandForBackup);

   private final InvocationComposeSuccessHandler readWriteManyEntriesPrimaryHandler
         = primaryReturnHandler(NonTxDistributionInterceptor::readWriteManyEntriesCommandForBackup);

   private final InvocationComposeSuccessHandler writeOnlyManyPrimaryHandler
         = primaryReturnHandler(NonTxDistributionInterceptor::writeOnlyManyCommandForBackup);

   private final InvocationComposeSuccessHandler writeOnlyManyEntriesPrimaryHandler
         = primaryReturnHandler(NonTxDistributionInterceptor::writeOnlyManyEntriesCommandForBackup);

   private final InvocationComposeSuccessHandler putMapReturnHandler
         = primaryReturnHandler(NonTxDistributionInterceptor::putMapCommandForBackup);

   private static ReadWriteManyCommand readWriteManyCommandForBackup(ReadWriteManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      ReadWriteManyCommand copy = new ReadWriteManyCommand(cmd);
      copy.setForwarded(true);
      copy.setKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      return copy;
   }

   private static ReadWriteManyEntriesCommand readWriteManyEntriesCommandForBackup(ReadWriteManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(cmd);
      copy.setForwarded(true);
      copy.setEntries(new ReadOnlySegmentAwareMap(cmd.getEntries(), ch, segments));
      return copy;
   }

   private static WriteOnlyManyCommand writeOnlyManyCommandForBackup(WriteOnlyManyCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      WriteOnlyManyCommand copy = new WriteOnlyManyCommand(cmd);
      copy.setForwarded(true);
      copy.setKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments));
      return copy;
   }

   private static WriteOnlyManyEntriesCommand writeOnlyManyEntriesCommandForBackup(WriteOnlyManyEntriesCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(cmd);
      copy.setForwarded(true);
      copy.setEntries(new ReadOnlySegmentAwareMap(cmd.getEntries(), ch, segments));
      return copy;
   }

   private static PutMapCommand putMapCommandForBackup(PutMapCommand cmd, ConsistentHash ch, Set<Integer> segments) {
      PutMapCommand copy = new PutMapCommand(cmd);
      copy.setForwarded(true);
      copy.setMap(new ReadOnlySegmentAwareMap(cmd.getMap(), ch, segments));
      return copy;
   }

   private static BiConsumer<MergingCompletableFuture<Object>, Object> moveListItemsToFuture(int myOffset) {
      return (f, rv) -> {
         if (rv instanceof List) {
            List list = (List) rv;
            if (trace) {
               log.tracef("Copying %d items %s to results (%s), starting offset %d", list.size(), list, Arrays.toString(f.results), myOffset);
            }
            for (int i = 0; i < list.size(); ++i) {
               f.results[myOffset + i] = list.get(i);
            }
         } else if (rv != null || f.results != null) {
            f.completeExceptionally(new IllegalArgumentException("Unexpected result value " + rv));
         }
      };
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand> InvocationComposeSuccessHandler primaryReturnHandler(CommandCopyFunction<C> copyForBackup) {
      return (stage, rCtx, rCommand, rv) -> {
         C original = (C) rCommand;
         ConsistentHash ch = checkTopologyId(original).getWriteConsistentHash();
         // We have already checked that the command topology is actual, so we can assume that we really are primary owner
         Map<Address, Set<Integer>> backups = backupOwnersOfSegments(ch, ch.getPrimarySegmentsForOwner(rpcManager.getAddress()));
         if (backups.isEmpty()) {
            return stage;
         }
         boolean isSync = isSynchronous(original);
         CompletableFuture[] futures = isSync ? new CompletableFuture[backups.size()] : null;
         int future = 0;
         for (Entry<Address, Set<Integer>> backup : backups.entrySet()) {
            C copy = copyForBackup.copy(original, ch, backup.getValue());
            if (isSync) {
               futures[future++] = rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultSyncOptions);
            } else {
               rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), copy, defaultAsyncOptions);
            }
         }
         return isSync ? returnWithAsync(CompletableFuture.allOf(futures).thenApply(nil -> rv)) : stage;
      };
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws
         Throwable {
      return handleNonTxWriteCommand(ctx, command);
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
   public BasicInvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command)
         throws Throwable {
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      if (ctx.isOriginLocal()) {
         if (entry != null) {
            // the entry is owned locally (it is NullCacheEntry if it was not found), no need to go remote
            return invokeNext(ctx, command);
         }
         if (readNeedsRemoteValue(ctx, command)) {
            CacheTopology cacheTopology = checkTopologyId(command);
            List<Address> owners = cacheTopology.getReadConsistentHash().locateOwners(key);
            if (trace)
               log.tracef("Doing a remote get for key %s in topology %d to %s", key, cacheTopology.getTopologyId(), owners);

            // make sure that the command topology is set to the value according which we route it
            command.setTopologyId(cacheTopology.getTopologyId());

            CompletableFuture<Map<Address, Response>> rpc = rpcManager.invokeRemotelyAsync(owners, command,
                  staggeredOptions);
            return returnWithAsync(rpc.thenApply(
                  (responseMap) -> {
                     for (Response rsp : responseMap.values()) {
                        if (rsp.isSuccessful()) {
                           return ((SuccessfulResponse) rsp).getResponseValue();
                        }
                     }
                     // On receiver side the command topology id is checked and if it's too new, the command is delayed.
                     // We can assume that we miss successful response only because the owners already have new topology
                     // in which they're not owners - we'll wait for this topology, then.
                     throw new OutdatedTopologyException("We haven't found an owner");
                  }));
         } else {
            // This has LOCAL flags, just wrap NullCacheEntry and let the command run
            entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), false, false);
            return invokeNext(ctx, command);
         }
      } else {
         if (entry == null) {
            // this is not an owner of the entry, don't pass to call interceptor at all
            return returnWith(UnsuccessfulResponse.INSTANCE);
         } else {
            return invokeNext(ctx, command);
         }
      }
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command)
         throws Throwable {
      return handleReadWriteManyCommand(ctx, command,
            (cmd, map) -> new PutMapCommand(cmd).withMap(map),
            (cmd, ch, segments) -> new PutMapCommand(cmd).withMap(new ReadOnlySegmentAwareMap<>(cmd.getMap(), ch, segments)),
            NonTxDistributionInterceptor::putMapCommandForBackup,
            cmd -> cmd.getMap().entrySet(), Entry::getKey, HashMap::new,
            (map, e) -> map.put(e.getKey(), e.getValue()), HashMap::size,
            (stage, rCommand, ch) -> {
               if (!rCommand.isForwarded() && ch.getNumOwners() > 1) {
                  return stage.thenCompose(putMapReturnHandler);
               } else {
                  return stage;
               }
            });
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                                WriteOnlyManyEntriesCommand command) throws Throwable {
      return this.<WriteOnlyManyEntriesCommand, HashMap, Entry>handleWriteOnlyManyCommand(ctx, command,
            (cmd, entries) -> new WriteOnlyManyEntriesCommand(cmd).withEntries(entries),
            (cmd, ch, segments) -> new WriteOnlyManyEntriesCommand(cmd)
                  .withEntries(new ReadOnlySegmentAwareMap<>(cmd.getEntries(), ch, segments)),
            NonTxDistributionInterceptor::writeOnlyManyEntriesCommandForBackup,
            cmd -> cmd.getEntries().entrySet(), Entry::getKey, HashMap::new,
            (map, e) -> map.put(e.getKey(), e.getValue()),
            command.isForwarded() ? null : writeOnlyManyEntriesPrimaryHandler);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyCommand(InvocationContext ctx,
                                                         WriteOnlyManyCommand command) throws Throwable {
      return handleWriteOnlyManyCommand(ctx, command,
            (cmd, keys) -> new WriteOnlyManyCommand(cmd).withKeys(keys),
            (cmd, ch, segments) -> new WriteOnlyManyCommand(cmd)
                  .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments)),
            NonTxDistributionInterceptor::writeOnlyManyCommandForBackup,
            WriteOnlyManyCommand::getAffectedKeys, Function.identity(), ArrayList::new, ArrayList::add,
            command.isForwarded() ? null : writeOnlyManyPrimaryHandler);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyCommand(InvocationContext ctx,
                                                         ReadWriteManyCommand command) throws Throwable {
      return handleReadWriteManyCommand(ctx, command,
            (cmd, keys) -> new ReadWriteManyCommand(cmd).withKeys(keys),
            (cmd, ch, segments) -> new ReadWriteManyCommand(cmd)
                  .withKeys(new ReadOnlySegmentAwareCollection(cmd.getAffectedKeys(), ch, segments)),
            NonTxDistributionInterceptor::readWriteManyCommandForBackup,
            ReadWriteManyCommand::getAffectedKeys, Function.identity(), ArrayList::new, ArrayList::add, ArrayList::size,
            (stage, rCommand, ch) -> {
               if (!rCommand.isForwarded() && ch.getNumOwners() > 1) {
                  return stage.thenCompose(readWriteManyPrimaryHandler);
               } else {
                  return stage;
               }
            });
   }

   @Override
   public BasicInvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                                ReadWriteManyEntriesCommand command) throws Throwable {
      return handleReadWriteManyCommand(ctx, command,
            (cmd, entries) -> new ReadWriteManyEntriesCommand(cmd).withEntries(entries),
            (cmd, ch, segments) -> new ReadWriteManyEntriesCommand(cmd)
                  .withEntries(new ReadOnlySegmentAwareMap<>(command.getEntries(), ch, segments)),
            NonTxDistributionInterceptor::readWriteManyEntriesCommandForBackup,
            cmd -> (Collection<Entry>) cmd.getEntries().entrySet(), entry -> entry.getKey(), HashMap::new,
            (entries, entry) -> entries.put(entry.getKey(), entry.getValue()), HashMap::size,
            (stage, rCommand, ch) -> {
               if (!rCommand.isForwarded() && ch.getNumOwners() > 1) {
                  return stage.thenCompose(readWriteManyEntriesPrimaryHandler);
               } else {
                  return stage;
               }
            });
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand, ItemAcc, Item> BasicInvocationStage handleWriteOnlyManyCommand(
         InvocationContext ctx, C command, BiFunction<C, ItemAcc, C> copyForLocal,
         CommandCopyFunction<C> copyForPrimary, CommandCopyFunction<C> copyForBackup,
         Function<C, Collection<Item>> currentItems, Function<Item, ?> item2key, Supplier<ItemAcc> newItemAcc,
         BiConsumer<ItemAcc, Item> accumulate,
         InvocationComposeSuccessHandler remoteReturnHandler) throws Exception {
      // TODO: due to possible repeating of the operation (after OutdatedTopologyException is thrown)
      // it is possible that the function will be applied multiple times on some of the nodes.
      // There is no general solution for this ATM; proper solution will probably record CommandInvocationId
      // in the entry, and implement some housekeeping
      ConsistentHash ch = checkTopologyId(command).getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, Set<Integer>> segmentMap = primaryOwnersOfSegments(ch);
         CountDownCompletableFuture allFuture = new CountDownCompletableFuture(ctx, segmentMap.size());
         InvocationStage returnStage = null;

         // Go through all members, for this node invokeNext (if this node is an owner of some keys),
         // for the others (that own some keys) issue a remote call.
         // Everything is finished when allFuture is completed
         for (Entry<Address, Set<Integer>> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            Set<Integer> segments = pair.getValue();
            if (member.equals(rpcManager.getAddress())) {
               // Filter command keys/entries into the collection, and wrap null for those that are not in context yet
               ItemAcc myItems = newItemAcc.get();
               for (Item item : currentItems.apply(command)) {
                  Object key = item2key.apply(item);
                  if (segments.contains(ch.getSegment(key))) {
                     accumulate.accept(myItems, item);
                     CacheEntry entry = ctx.lookupEntry(key);
                     if (entry == null) {
                        entryFactory.wrapExternalEntry(ctx, key, null, true, true);
                     }
                  }
               }

               C localCommand = copyForLocal.apply(command, myItems);
               // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
               // calls complete.
               returnStage = invokeNext(ctx, localCommand).handle(
                     createLocalInvocationHandler(ch, allFuture, segments, copyForBackup, currentItems, (f, rv) -> {
                     }));
               continue;
            }

            C copy = copyForPrimary.copy(command, ch, segments);
            int size = currentItems.apply(copy).size();
            if (size <= 0) {
               allFuture.countDown();
               continue;
            }

            rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, defaultSyncOptions).whenComplete((responseMap, throwable) -> {
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
         if (returnStage == null) {
            return returnWithAsync(allFuture);
         } else {
            return returnWithAsync(returnStage.toCompletableFuture().thenCompose(localResult -> allFuture));
         }
      } else { // origin is not local
         // check that we have all the data we need
         for (Item item : currentItems.apply(command)) {
            Object key = item2key.apply(item);
            if (ctx.lookupEntry(key) == null) {
               entryFactory.wrapExternalEntry(ctx, key, null, true, true);
            }
         }

         if (remoteReturnHandler != null) {
            return invokeNext(ctx, command).thenCompose(remoteReturnHandler);
         } else {
            return invokeNext(ctx, command);
         }
      }
   }

   private <C extends FlagAffectedCommand & TopologyAffectedCommand, ItemAcc, Item> BasicInvocationStage handleReadWriteManyCommand(
         InvocationContext ctx, C command, BiFunction<C, ItemAcc, C> copyForLocal,
         CommandCopyFunction<C> copyForPrimary, CommandCopyFunction<C> copyForBackup,
         Function<C, Collection<Item>> currentItems, Function<Item, ?> item2key, Supplier<ItemAcc> newItemAcc,
         BiConsumer<ItemAcc, Item> accumulate, Function<ItemAcc, Integer> accSize,
         RemoteReturnHandlerRegistation<C> remoteReturnHandlerRegistration) throws Exception {
      // TODO: due to possible repeating of the operation (after OutdatedTopologyException is thrown)
      // it is possible that the function will be applied multiple times on some of the nodes.
      // There is no general solution for this ATM; proper solution will probably record CommandInvocationId
      // in the entry, and implement some housekeeping
      ConsistentHash ch = checkTopologyId(command).getWriteConsistentHash();
      if (ctx.isOriginLocal()) {
         Map<Address, Set<Integer>> segmentMap = primaryOwnersOfSegments(ch);
         Object[] results = null;
         Function<Object[], Object> transform = null;
         if (!command.hasFlag(Flag.IGNORE_RETURN_VALUES)) {
            results = new Object[currentItems.apply(command).size()];
            transform = Arrays::asList;
         }
         MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(ctx, segmentMap.size(), results, transform);
         InvocationStage returnStage = null;
         int offset = 0;

         // Go through all members, for this node invokeNext (if this node is an owner of some keys),
         // for the others (that own some keys) issue a remote call.
         // Everything is finished when allFuture is completed
         for (Entry<Address, Set<Integer>> pair : segmentMap.entrySet()) {
            Address member = pair.getKey();
            Set<Integer> segments = pair.getValue();
            if (member.equals(rpcManager.getAddress())) {
               ItemAcc myItems = newItemAcc.get();
               List<CompletableFuture<?>> retrievals = null;
               // Filter command keys/entries into the collection, and record remote retrieval for those that are not
               // in the context yet
               for (Item item : currentItems.apply(command)) {
                  Object key = item2key.apply(item);
                  if (segments.contains(ch.getSegment(key))) {
                     accumulate.accept(myItems, item);
                     retrievals = addRemoteGet(ctx, command, retrievals, key);
                  }
               }
               int size = accSize.apply(myItems);
               if (size == 0) {
                  allFuture.countDown();
                  continue;
               }
               final int myOffset = offset;
               offset += size;

               C localCommand = copyForLocal.apply(command, myItems);
               if (retrievals == null) {
                  returnStage = invokeNext(ctx, localCommand);
               } else {
                  // We must wait until all retrievals finish before proceeding with the local command
                  CompletableFuture[] ra = retrievals.toArray(new CompletableFuture[retrievals.size()]);
                  returnStage = invokeNextAsync(ctx, localCommand, CompletableFuture.allOf(ra));
               }
               // Local keys are backed up in the handler, and counters on allFuture are decremented when the backup
               // calls complete.
               returnStage = returnStage.handle(createLocalInvocationHandler(ch, allFuture, segments, copyForBackup,
                     currentItems, moveListItemsToFuture(myOffset)));
            } else {
               final int myOffset = offset;
               // TODO: here we iterate through all entries - is the ReadOnlySegmentAwareMap really worth it?
               C copy = copyForPrimary.copy(command, ch, segments);
               int size = currentItems.apply(copy).size();
               offset += size;
               if (size <= 0) {
                  allFuture.countDown();
                  continue;
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
         }
         if (returnStage == null) {
            return returnWithAsync(allFuture);
         } else {
            return returnWithAsync(returnStage.toCompletableFuture().thenCompose(localResult -> allFuture));
         }
      } else { // origin is not local
         List<CompletableFuture<?>> retrievals = null;
         // check that we have all the data we need
         for (Item item : currentItems.apply(command)) {
            retrievals = addRemoteGet(ctx, command, retrievals, item2key.apply(item));
         }

         InvocationStage returnStage;
         if (retrievals != null) {
            CompletableFuture[] ra = retrievals.toArray(new CompletableFuture[retrievals.size()]);
            returnStage = invokeNextAsync(ctx, command, CompletableFuture.allOf(ra));
         } else {
            returnStage = invokeNext(ctx, command);
         }
         return remoteReturnHandlerRegistration.apply(returnStage, command, ch);
      }
   }

   private List<CompletableFuture<?>> addRemoteGet(InvocationContext ctx, FlagAffectedCommand command, List<CompletableFuture<?>> retrievals, Object key) throws Exception {
      CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (cacheEntry == null) {
         // this should be a rare situation, so we don't mind being a bit ineffective with the remote gets
         if (command.hasFlag(Flag.SKIP_REMOTE_LOOKUP) || command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
            entryFactory.wrapExternalEntry(ctx, key, null, true, false);
         } else {
            if (retrievals == null) {
               retrievals = new ArrayList<>();
            }
            GetCacheEntryCommand fakeGetCommand = cf.buildGetCacheEntryCommand(key, command.getFlagsBitSet());
            CompletableFuture<?> getFuture = remoteGet(ctx, fakeGetCommand, true);
            retrievals.add(getFuture);
         }
      }
      return retrievals;
   }

   private <C extends FlagAffectedCommand, F extends CountDownCompletableFuture, Item> InvocationFinallyHandler createLocalInvocationHandler(
         ConsistentHash ch, F allFuture,
         Set<Integer> segments, CommandCopyFunction<C> copyForBackup, Function<C, Collection<Item>> currentItems,
         BiConsumer<F, Object> returnValueConsumer) {
      return (rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            returnValueConsumer.accept(allFuture, rv);

            Map<Address, Set<Integer>> backupOwners = backupOwnersOfSegments(ch, segments);
            for (Entry<Address, Set<Integer>> backup : backupOwners.entrySet()) {
               // rCommand is the original command
               C backupCopy = copyForBackup.copy((C) rCommand, ch, backup.getValue());
               if (currentItems.apply(backupCopy).isEmpty()) continue;
               if (isSynchronous(backupCopy)) {
                  allFuture.increment();
                  rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), backupCopy, defaultSyncOptions).whenComplete((responseMap, remoteThrowable) -> {
                     if (remoteThrowable != null) {
                        allFuture.completeExceptionally(remoteThrowable);
                     } else {
                        allFuture.countDown();
                     }
                  });
               } else {
                  rpcManager.invokeRemotelyAsync(Collections.singleton(backup.getKey()), backupCopy, defaultAsyncOptions);
               }
            }
            allFuture.countDown();
         } catch (Throwable t) {
            allFuture.completeExceptionally(t);
         }
      };
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   private interface CommandCopyFunction<C extends VisitableCommand> {
      C copy(C cmd, ConsistentHash ch, Set<Integer> segments);
   }

   private interface RemoteReturnHandlerRegistation<C extends VisitableCommand> {
      InvocationStage apply(InvocationStage stage, C command, ConsistentHash ch);
   }
}
