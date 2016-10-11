package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.RemoteFetchingCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.util.ReadOnlySegmentAwareCollection;
import org.infinispan.distribution.util.ReadOnlySegmentAwareMap;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.rpc.RpcOptions;
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

   @Override
   public BasicInvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws
         Throwable {
      return visitGetCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable {
      return visitGetCommand(ctx, command);
   }

   private <T extends AbstractDataCommand & RemoteFetchingCommand> BasicInvocationStage visitGetCommand(
         InvocationContext ctx, T command) throws Throwable {
      if (!ctx.isOriginLocal())
         return invokeNext(ctx, command);

      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      if (valueIsMissing(entry)) {
         if (readNeedsRemoteValue(ctx, command)) {
            if (trace)
               log.tracef("Doing a remote get for key %s", key);
            CompletableFuture<InternalCacheEntry> remoteFuture = retrieveFromProperSource(key, ctx, command, false);
            return invokeNextAsync(ctx, command, remoteFuture.thenAccept(remoteEntry -> {
               command.setRemotelyFetchedValue(remoteEntry);
               handleRemoteEntry(ctx, key, remoteEntry);
            }));
         }
      }
      return invokeNext(ctx, command);
   }

   private void handleRemoteEntry(InvocationContext ctx, Object key, InternalCacheEntry remoteEntry) {
      if (remoteEntry != null) {
         entryFactory.wrapExternalEntry(ctx, key, remoteEntry, EntryFactory.Wrap.STORE, false);
      }
   }

   @Override
   public BasicInvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws
         Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      Map<Object, Object> originalMap = command.getMap();
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures =
               new ArrayList<>(rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  PutMapCommand copy = new PutMapCommand(command);
                  copy.setMap(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future =
                        rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
               throw new RemoteException("Exception while processing put on primary owner", e.getCause());
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.addFlag(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  PutMapCommand copy = new PutMapCommand(command);
                  copy.setMap(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future = rpcManager
                        .invokeRemotelyAsync(Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  throw new RemoteException("Exception while processing put on backup owner", e.getCause());
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNext(ctx, command);
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
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
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
            entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), EntryFactory.Wrap.STORE, false);
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
   public BasicInvocationStage visitWriteOnlyManyEntriesCommand(InvocationContext ctx,
                                                                WriteOnlyManyEntriesCommand command)
         throws Throwable {
      // TODO: Refactor this and visitPutMapCommand...
      // TODO: Could PutMap be reimplemented based on WriteOnlyManyEntriesCommand?
      Map<Object, Object> originalMap = command.getEntries();
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures =
               new ArrayList<>(rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(command);
                  copy.setEntries(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future =
                        rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
               throw new RemoteException("Exception while processing put on primary owner", e.getCause());
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  WriteOnlyManyEntriesCommand copy = new WriteOnlyManyEntriesCommand(command);
                  copy.setEntries(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future = rpcManager
                        .invokeRemotelyAsync(Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  if (e.getCause() instanceof RemoteException) {
                     throw e.getCause();
                  } else {
                     throw new RemoteException("Exception while processing put on backup owner", e.getCause());
                  }
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable {
      // TODO: Refactor this, visitWriteOnlyManyCommand and visitPutMapCommand...
      Collection<?> originalMap = command.getAffectedKeys();
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures =
               new ArrayList<>(rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Collection<?> segmentKeysSet = new ReadOnlySegmentAwareCollection<>(originalMap, ch, segments);
               if (!segmentKeysSet.isEmpty()) {
                  WriteOnlyManyCommand copy = new WriteOnlyManyCommand(command);
                  copy.setKeys(segmentKeysSet);
                  CompletableFuture<Map<Address, Response>> future =
                        rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
               Throwable cause = e.getCause();
               if (cause instanceof RemoteException) {
                  throw cause.getCause();
               }
               throw new RemoteException("Exception while processing put on primary owner", cause);
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Collection<?> segmentKeysSet = new ReadOnlySegmentAwareCollection<>(originalMap, ch, segments);
               if (!segmentKeysSet.isEmpty()) {
                  WriteOnlyManyCommand copy = new WriteOnlyManyCommand(command);
                  copy.setKeys(segmentKeysSet);
                  CompletableFuture<Map<Address, Response>> future = rpcManager
                        .invokeRemotelyAsync(Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  if (e.getCause() instanceof RemoteException) {
                     throw e.getCause();
                  } else {
                     throw new RemoteException("Exception while processing put on backup owner", e.getCause());
                  }
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable {
      // TODO: Refactor to avoid code duplication
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures =
               new ArrayList<>(rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Collection<Object> segmentKeysSet = new ReadOnlySegmentAwareCollection<>(command.getAffectedKeys(), ch, segments);
               if (!segmentKeysSet.isEmpty()) {
                  ReadWriteManyCommand copy = new ReadWriteManyCommand(command);
                  copy.setKeys(segmentKeysSet);
                  CompletableFuture<Map<Address, Response>> future =
                        rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               // NOTE: Variation from WriteOnlyManyCommand, we care about returns!
               // TODO: Take into account when refactoring
               for (CompletableFuture<Map<Address, Response>> future : futures) {
                  Map<Address, Response> responses = future.get();
                  for (Response response : responses.values()) {
                     if (response.isSuccessful()) {
                        SuccessfulResponse success = (SuccessfulResponse) response;
                        command.addAllRemoteReturns((List<?>) success.getResponseValue());
                     }
                  }
               }
            } catch (ExecutionException e) {
               if (e.getCause() instanceof RemoteException) {
                  throw e.getCause();
               } else {
                  throw new RemoteException("Exception while processing put on primary owner", e.getCause());
               }
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Collection<?> segmentKeysSet = new ReadOnlySegmentAwareCollection<>(command.getAffectedKeys(), ch, segments);
               if (!segmentKeysSet.isEmpty()) {
                  ReadWriteManyCommand copy = new ReadWriteManyCommand(command);
                  copy.setKeys(segmentKeysSet);
                  CompletableFuture<Map<Address, Response>> future = rpcManager
                        .invokeRemotelyAsync(Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  if (e.getCause() instanceof RemoteException) {
                     throw e.getCause();
                  } else {
                     throw new RemoteException("Exception while processing put on backup owner", e.getCause());
                  }
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitReadWriteManyEntriesCommand(InvocationContext ctx,
                                                                ReadWriteManyEntriesCommand command)
         throws Throwable {
      // TODO: Refactor to avoid code duplication
      Map<Object, Object> originalMap = command.getEntries();
      ConsistentHash ch = dm.getConsistentHash();
      Address localAddress = rpcManager.getAddress();
      if (ctx.isOriginLocal()) {
         List<CompletableFuture<Map<Address, Response>>> futures =
               new ArrayList<>(rpcManager.getMembers().size() - 1);
         // TODO: if async we don't need to do futures...
         RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
         for (Address member : rpcManager.getMembers()) {
            if (member.equals(rpcManager.getAddress())) {
               continue;
            }
            Set<Integer> segments = ch.getPrimarySegmentsForOwner(member);
            if (!segments.isEmpty()) {
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(command);
                  copy.setEntries(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future =
                        rpcManager.invokeRemotelyAsync(Collections.singletonList(member), copy, options);
                  futures.add(future);
               }
            }
         }
         if (futures.size() > 0) {
            CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
            CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
            try {
               compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               // NOTE: Variation from WriteOnlyManyCommand, we care about returns!
               // TODO: Take into account when refactoring
               for (CompletableFuture<Map<Address, Response>> future : futures) {
                  Map<Address, Response> responses = future.get();
                  for (Response response : responses.values()) {
                     if (response.isSuccessful()) {
                        SuccessfulResponse success = (SuccessfulResponse) response;
                        command.addAllRemoteReturns((List<?>) success.getResponseValue());
                     }
                  }
               }
            } catch (ExecutionException e) {
               if (e.getCause() instanceof RemoteException) {
                  throw e.getCause();
               } else {
                  throw new RemoteException("Exception while processing put on primary owner", e.getCause());
               }
            } catch (TimeoutException e) {
               throw new CacheException(e);
            }
         }
      }

      if (!command.isForwarded() && ch.getNumOwners() > 1) {
         // Now we find all the segments that we own and map our backups to those
         Map<Address, Set<Integer>> backupOwnerSegments = new HashMap<>();
         int segmentCount = ch.getNumSegments();
         for (int i = 0; i < segmentCount; ++i) {
            Iterator<Address> iter = ch.locateOwnersForSegment(i).iterator();

            if (iter.next().equals(localAddress)) {
               while (iter.hasNext()) {
                  Address backupOwner = iter.next();
                  Set<Integer> segments = backupOwnerSegments.get(backupOwner);
                  if (segments == null) {
                     backupOwnerSegments.put(backupOwner, (segments = new HashSet<>()));
                  }
                  segments.add(i);
               }
            }
         }

         int backupOwnerSize = backupOwnerSegments.size();
         if (backupOwnerSize > 0) {
            List<CompletableFuture<Map<Address, Response>>> futures = new ArrayList<>(backupOwnerSize);
            RpcOptions options = rpcManager.getDefaultRpcOptions(isSynchronous(command));
            command.setFlags(Flag.SKIP_LOCKING);
            command.setForwarded(true);

            for (Entry<Address, Set<Integer>> entry : backupOwnerSegments.entrySet()) {
               Set<Integer> segments = entry.getValue();
               Map<Object, Object> segmentEntriesMap =
                     new ReadOnlySegmentAwareMap<>(originalMap, ch, segments);
               if (!segmentEntriesMap.isEmpty()) {
                  ReadWriteManyEntriesCommand copy = new ReadWriteManyEntriesCommand(command);
                  copy.setEntries(segmentEntriesMap);
                  CompletableFuture<Map<Address, Response>> future = rpcManager
                        .invokeRemotelyAsync(Collections.singletonList(entry.getKey()), copy, options);
                  futures.add(future);
               }
            }
            command.setForwarded(false);
            if (futures.size() > 0) {
               CompletableFuture[] futuresArray = new CompletableFuture[futures.size()];
               CompletableFuture<Void> compFuture = CompletableFuture.allOf(futures.toArray(futuresArray));
               try {
                  compFuture.get(options.timeout(), TimeUnit.MILLISECONDS);
               } catch (ExecutionException e) {
                  if (e.getCause() instanceof RemoteException) {
                     throw e.getCause();
                  } else {
                     throw new RemoteException("Exception while processing put on backup owner", e.getCause());
                  }
               } catch (TimeoutException e) {
                  throw new CacheException(e);
               }
            }
         }
      }

      return invokeNext(ctx, command);
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

   @Override
   protected CompletableFuture<?> remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, Object key)
         throws Throwable {
      CacheEntry entry = ctx.lookupEntry(key);
      if (!valueIsMissing(entry)) {
         return null;
      }
      CompletableFuture<InternalCacheEntry> remoteFuture;
      if (writeNeedsRemoteValue(ctx, command, key)) {
         int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
         int cmdTopology = command.getTopologyId();
         boolean topologyChanged = currentTopologyId != cmdTopology && cmdTopology != -1;
         if (topologyChanged) {
            throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                    cmdTopology + ", got " + currentTopologyId);
         }
         remoteFuture = retrieveFromProperSource(key, ctx, command, false);
         return remoteFuture.thenAccept(remoteEntry -> {
            handleRemoteEntry(ctx, key, remoteEntry);
         });
      }
      return null;
   }

   @Override
   protected boolean writeNeedsRemoteValue(InvocationContext ctx, WriteCommand command, Object key) {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         return false;
      }
      if (ctx.isOriginLocal() && command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         // Ignore SKIP_REMOTE_LOOKUP if we're already remote
         return false;
      }
      // Most of the time, the previous value only matters on the primary owner,
      // and we always have the existing value on the primary owner.
      // For DeltaAware writes we need the previous value on all the owners.
      // But if the originator is executing the command directly, it means it's the primary owner
      // and so it has the existing value already.
      return !ctx.isOriginLocal() && command.alwaysReadsExistingValues();
   }
}
