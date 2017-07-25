package org.infinispan.interceptors.distribution;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ByRef;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.NotifyHelper;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This interceptor mixes several functions:
 * A) replicates changes to other nodes
 * B) commits the entry
 * C) schedules invalidation
 *
 * On primary owner, the commit is executed before the change is replicated to other node. If the command
 * reads previous value and the version of entry in {@link org.infinispan.container.DataContainer} has changed
 * during execution {@link ConcurrentChangeException} is thrown and the command has to be retried.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredDistributionInterceptor extends ClusteringInterceptor {
   private final static Log log = LogFactory.getLog(ScatteredDistributionInterceptor.class);
   private final static boolean trace = log.isTraceEnabled();

   protected ScatteredVersionManager<Object> svm;
   protected GroupManager groupManager;
   protected TimeService timeService;
   protected CacheNotifier cacheNotifier;
   protected FunctionalNotifier functionalNotifier;
   protected KeyPartitioner keyPartitioner;
   protected DistributionManager distributionManager;

   private volatile Address cachedNextMember;
   private volatile int cachedNextMemberTopology = -1;

   private final InvocationSuccessAction dataWriteCommandNoReadHandler = (rCtx, rCommand, rv) -> {
      DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
      CacheEntry entry = rCtx.lookupEntry(dataWriteCommand.getKey());
      boolean committed = commitSingleEntryIfNewer(entry, rCtx, dataWriteCommand);
      if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), entry.getMetadata().version(), entry.isRemoved());
      }
   };

   private final InvocationSuccessAction putMapCommandHandler = (rCtx, rCommand, rv) -> {
      PutMapCommand putMapCommand = (PutMapCommand) rCommand;
      for (Object key : putMapCommand.getAffectedKeys()) {
         commitSingleEntryIfNewer(rCtx.lookupEntry(key), rCtx, rCommand);
         // this handler is called only for ST or when isOriginLocal() == false so we don't have to invalidate
      }
   };

   private final InvocationSuccessAction clearHandler = (rCtx, rCommand, rv) -> {
      List<InternalCacheEntry<Object, Object>> copyEntries = new ArrayList<>(dataContainer.entrySet());
      dataContainer.clear();
      for (InternalCacheEntry entry : copyEntries) {
         cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, rCtx, (ClearCommand) rCommand);
      }
   };
   private InvocationSuccessFunction handleWritePrimaryResponse = this::handleWritePrimaryResponse;

   @Inject
   public void injectDependencies(GroupManager groupManager, ScatteredVersionManager svm, TimeService timeService,
                                  CacheNotifier cacheNotifier, FunctionalNotifier functionalNotifier, KeyPartitioner keyPartitioner,
                                  DistributionManager distributionManager) {
      this.groupManager = groupManager;
      this.svm = svm;
      this.timeService = timeService;
      this.cacheNotifier = cacheNotifier;
      this.functionalNotifier = functionalNotifier;
      this.keyPartitioner = keyPartitioner;
      this.distributionManager = distributionManager;
   }

   private <T extends DataWriteCommand & MetadataAwareCommand> Object handleWriteCommand(InvocationContext ctx, T command) throws Throwable {
      CacheEntry cacheEntry = ctx.lookupEntry(command.getKey());
      EntryVersion seenVersion = getVersionOrNull(cacheEntry);
      LocalizedCacheTopology cacheTopology = checkTopology(command);

      DistributionInfo info = cacheTopology.getDistribution(command.getKey());
      if (info.primary() == null) {
         throw OutdatedTopologyException.INSTANCE;
      }

      if (isLocalModeForced(command)) {
         CacheEntry contextEntry = cacheEntry;
         if (cacheEntry == null) {
            entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
            contextEntry = ctx.lookupEntry(command.getKey());
         }
         if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            contextEntry.setMetadata(command.getMetadata());
         } else if (info.isPrimary()) {
            if (cacheTopology.getTopologyId() == 0) {
               // this is the singleton topology used for preload
               contextEntry.setMetadata(command.getMetadata());
               svm.updatePreloadedEntryVersion(command.getMetadata().version());
            } else {
               // let's allow local-mode writes on primary owner, preserving versions
               EntryVersion nextVersion = svm.incrementVersion(info.segmentId());
               contextEntry.setMetadata(addVersion(command.getMetadata(), nextVersion));
            }
         }
         return commitSingleEntryOnReturn(ctx, command, contextEntry, contextEntry.getValue(), seenVersion);
      }

      if (ctx.isOriginLocal()) {
         if (info.isPrimary()) {
            Object seenValue = cacheEntry.getValue();
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
               handleWriteOnOriginPrimary(rCtx, (T) rCommand, rv, cacheEntry, seenValue, seenVersion, cacheTopology, info));
         } else { // not primary owner
            CompletableFuture<Map<Address, Response>> rpcFuture =
               rpcManager.invokeRemotelyAsync(info.writeOwners(), command, defaultSyncOptions);
            return asyncValue(rpcFuture).thenApply(ctx, command, handleWritePrimaryResponse);
         }
      } else { // remote origin
         if (info.isPrimary()) {
            Object seenValue = cacheEntry.getValue();
            // TODO [ISPN-3918]: the previous value is unreliable as this could be second invocation
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               T cmd = (T) rCommand;
               if (!cmd.isSuccessful()) {
                  if (trace) log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", cmd);
                  return rv;
               }

               EntryVersion nextVersion = svm.incrementVersion(info.segmentId());
               Metadata metadata = addVersion(cmd.getMetadata(), nextVersion);
               cacheEntry.setMetadata(metadata);
               cmd.setMetadata(metadata);

               if (cmd.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
                  commitSingleEntryIfNoChange(seenValue, seenVersion, cacheEntry, rCtx, cmd);
               } else {
                  commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
               }

               if (cmd.isReturnValueExpected()) {
                  return new MetadataImmortalCacheValue(rv, metadata);
               } else {
                  // force return value to be sent in the response (the version)
                  command.setFlagsBitSet(command.getFlagsBitSet() & ~(FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP));
                  return metadata.version();
               }
            });
         } else {
            // The origin is primary and we're merely backup saving the data
            assert cacheEntry == null || command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
            CacheEntry contextEntry;
            if (cacheEntry == null) {
               entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
               contextEntry = ctx.lookupEntry(command.getKey());
            } else {
               contextEntry = cacheEntry;
            }
            contextEntry.setMetadata(command.getMetadata());
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               commitSingleEntryIfNewer(contextEntry, rCtx, rCommand);
               return null;
            });
         }
      }
   }

   private <T extends DataWriteCommand & MetadataAwareCommand> Object handleWriteOnOriginPrimary(InvocationContext ctx, T command, Object rv,
                                                                                                 CacheEntry cacheEntry, Object seenValue, EntryVersion seenVersion,
                                                                                                 CacheTopology cacheTopology, DistributionInfo info) {
      if (!command.isSuccessful()) {
         if (trace)
            log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
         return rv;
      }

      // increment the version
      EntryVersion nextVersion = svm.incrementVersion(info.segmentId());
      Metadata metadata = addVersion(command.getMetadata(), nextVersion);
      cacheEntry.setMetadata(metadata);
      command.setMetadata(metadata);

      boolean committed;
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
         committed = commitSingleEntryIfNoChange(seenValue, seenVersion, cacheEntry, ctx, command);
      } else {
         committed = commitSingleEntryIfNewer(cacheEntry, ctx, command);
      }

      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      // When replicating to backup, we'll add skip ownership check since we're now on primary owner
      // and we have already committed the entry, reading the return value. If we got OTE from remote
      // site and the command would be retried, we could fail to do the retry/return wrong value.
      // TODO: maybe we should rather create a copy of the command with modifications...
      command.addFlags(FlagBitSets.SKIP_OWNERSHIP_CHECK);
      Address backup = getNextMember(cacheTopology);
      if (backup != null) {
         // error responses throw exceptions from JGroupsTransport
         CompletableFuture<Map<Address, Response>> rpcFuture =
            rpcManager.invokeRemotelyAsync(Collections.singletonList(backup), command, defaultSyncOptions);
         rpcFuture.thenRun(() -> {
            if (committed && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               svm.scheduleKeyInvalidation(command.getKey(), cacheEntry.getMetadata().version(), cacheEntry.isRemoved());
            }
         });
         // Exception responses are thrown anyway and we don't expect any return values
         return asyncValue(rpcFuture.thenApply(ignore -> rv));
      } else {
         return rv;
      }
   }

   private Object handleWritePrimaryResponse(InvocationContext ctx, VisitableCommand command, Object rv) throws Throwable {
      Response response = getSingleResponse((Map<Address, Response>) rv);
      if (!response.isSuccessful()) {
         ((DataWriteCommand) command).fail();
         if (response instanceof UnsuccessfulResponse) {
            return ((UnsuccessfulResponse) response).getResponseValue();
         } else {
            throw new CacheException("Unexpected response " + response);
         }
      }

      Object responseValue = ((SuccessfulResponse) response).getResponseValue();
      return command.acceptVisitor(ctx, new PrimaryResponseHandler(responseValue));
   }

   private <T extends FlagAffectedCommand & TopologyAffectedCommand> LocalizedCacheTopology checkTopology(T command) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      assert command.getTopologyId() >= 0;
      if (!command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL) && command.getTopologyId() != cacheTopology.getTopologyId()) {
         // When this exception is thrown and the topology is installed before we handle this in StateTransferInterceptor,
         // we would wait for topology with id that will never come (due to +1).
         // Note that this does not happen to write commands as these are not processed until we receive the topology
         // these request.
         throw new OutdatedTopologyException(command.getTopologyId());
      } else if (trace) {
         log.tracef("%s has topology %d (current is %d)", command, command.getTopologyId(), cacheTopology.getTopologyId());
      }
      return cacheTopology;
   }

   private Object commitSingleEntryOnReturn(InvocationContext ctx, DataWriteCommand command, CacheEntry cacheEntry, Object prevValue, EntryVersion prevVersion) {
      if (command.loadType() != VisitableCommand.LoadType.DONT_LOAD) {
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
            boolean committed = commitSingleEntryIfNoChange(prevValue, prevVersion, cacheEntry, rCtx, rCommand);
            if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), cacheEntry.getMetadata().version(), cacheEntry.isRemoved());
            }
         });
      } else {
         return invokeNextThenAccept(ctx, command, dataWriteCommandNoReadHandler);
      }
   }

   private boolean commitSingleEntryIfNewer(CacheEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
            return false;
         }
      }
      // ignore metadata argument and use the one from entry, as e.g. PutMapCommand passes its metadata
      // here and we need own metadata for each entry.
      // RemoveCommand does not null the entry value
      if (entry.isRemoved()) {
         entry.setValue(null);
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
      ByRef<Object> previousValue = new ByRef<>(null);
      ByRef<Metadata> previousMetadata = new ByRef<>(null);
      ByRef.Boolean successful = new ByRef.Boolean(false);
      dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
         // newMetadata is null in case of local-mode write
         Metadata newMetadata = entry.getMetadata();
         if (oldEntry == null) {
            if (entry.getValue() == null && newMetadata == null) {
               if (trace) {
                  log.trace("No previous record and this is a removal, not committing anything.");
               }
               return null;
            } else {
               if (trace) {
                  log.trace("Committing new entry " + entry);
               }
               successful.set(true);
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         InequalVersionComparisonResult comparisonResult;
         if (oldMetadata == null || oldMetadata.version() == null || newMetadata == null || newMetadata.version() == null
            || (comparisonResult = oldMetadata.version().compareTo(newMetadata.version())) == InequalVersionComparisonResult.BEFORE
            || (oldMetadata instanceof RemoteMetadata && comparisonResult == InequalVersionComparisonResult.EQUAL)) {
            previousValue.set(oldEntry.getValue());
            previousValue.set(oldMetadata);
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            successful.set(true);
            if (entry.getValue() != null || newMetadata != null) {
               return factory.create(entry);
            } else {
               return null;
            }
         } else {
            if (trace) {
               log.tracef("Not committing %s, current entry is %s", entry, oldEntry);
            }
            return oldEntry;
         }
      });

      boolean created = entry.isCreated();
      boolean removed = entry.isRemoved();
      boolean expired = false;
      if (removed && entry instanceof MVCCEntry) {
         expired = ((MVCCEntry) entry).isExpired();
      }

      if (successful.get()) {
         NotifyHelper.entryCommitted(cacheNotifier, functionalNotifier, created, removed, expired,
               entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         // we skip the notification, and the already executed notification skipped this (intermediate) update
         return false;
      }
   }

   private boolean commitSingleEntryIfNoChange(Object seenValue, EntryVersion seenVersion, CacheEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
            return false;
         }
      }
      // ignore metadata argument and use the one from entry, as e.g. PutMapCommand passes its metadata
      // here and we need own metadata for each entry.
      // RemoveCommand does not null the entry value
      if (entry.isRemoved()) {
         entry.setValue(null);
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
      ByRef<Object> previousValue = new ByRef<>(null);
      ByRef<Metadata> previousMetadata = new ByRef<>(null);
      ByRef.Boolean successful = new ByRef.Boolean(false);
      dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
         // newMetadata is null in case of local-mode write on non-primary owners
         Metadata newMetadata = entry.getMetadata();
         if (oldEntry == null) {
            if (seenValue != null) {
               if (trace) {
                  log.trace("Non-null value in context, not committing");
               }
               throw new ConcurrentChangeException();
            }
            if (entry.getValue() == null && newMetadata == null) {
               if (trace) {
                  log.trace("No previous record and this is a removal, not committing anything.");
               }
               return null;
            } else {
               if (trace) {
                  log.trace("Committing new entry " + entry);
               }
               successful.set(true);
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         EntryVersion oldVersion = oldMetadata == null ? null : oldMetadata.version();
         if (oldVersion == null) {
            if (seenVersion != null) {
               if (trace) {
                  log.tracef("Current version is null but seen version is %s, throwing", seenVersion);
               }
               throw new ConcurrentChangeException();
            }
         } else if (seenVersion == null) {
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               if (trace) {
                  log.trace("Current entry is expired and therefore we haven't seen it");
               }
            } else {
               if (trace) {
                  log.tracef("Current version is %s but seen version is null, throwing", oldVersion);
               }
               throw new ConcurrentChangeException();
            }
         } else if (seenVersion.compareTo(oldVersion) != InequalVersionComparisonResult.EQUAL) {
            if (trace) {
               log.tracef("Current version is %s but seen version is %s, throwing", oldVersion, seenVersion);
            }
            throw new ConcurrentChangeException();
         }
         InequalVersionComparisonResult comparisonResult;
         if (oldVersion == null || newMetadata == null || newMetadata.version() == null
            || (comparisonResult = oldMetadata.version().compareTo(newMetadata.version())) == InequalVersionComparisonResult.BEFORE
            || (oldMetadata instanceof RemoteMetadata && comparisonResult == InequalVersionComparisonResult.EQUAL)) {
            previousValue.set(oldEntry.getValue());
            previousValue.set(oldMetadata);
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            successful.set(true);
            if (entry.getValue() != null || newMetadata != null) {
               return factory.create(entry);
            } else {
               return null;
            }
         } else {
            if (trace) {
               log.tracef("Not committing %s, current entry is %s", entry, oldEntry);
            }
            return oldEntry;
         }
      });

      boolean created = entry.isCreated();
      boolean removed = entry.isRemoved();
      boolean expired = false;
      if (removed && entry instanceof MVCCEntry) {
         expired = ((MVCCEntry) entry).isExpired();
      }

      if (successful.get()) {
         NotifyHelper.entryCommitted(cacheNotifier, functionalNotifier, created, removed, expired,
               entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         // we skip the notification, and the already executed notification skipped this (intermediate) update
         return false;
      }
   }

   private EntryVersion getVersionOrNull(CacheEntry cacheEntry) {
      if (cacheEntry == null) {
         return null;
      }
      Metadata metadata = cacheEntry.getMetadata();
      if (metadata != null) {
         return metadata.version();
      }
      return null;
   }

   private static Metadata addVersion(Metadata metadata, EntryVersion nextVersion) {
      Metadata.Builder builder;
      if (metadata == null) {
         builder = new EmbeddedMetadata.Builder();
      } else {
         builder = metadata.builder();
      }
      metadata = builder.version(nextVersion).build();
      return metadata;
   }

   private Address getNextMember(CacheTopology cacheTopology) {
      if (cacheTopology.getTopologyId() == cachedNextMemberTopology) {
         return cachedNextMember;
      }
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      List<Address> members = ch.getMembers();
      Address address = rpcManager.getAddress();
      Address nextMember = null;
      if (members.size() > 1) {
         for (int i = 0; i < members.size(); ++i) {
            Address member = members.get(i);
            if (member.equals(address)) {
               if (i + 1 < members.size()) {
                  nextMember = members.get(i + 1);
               } else {
                  nextMember = members.get(0);
               }
               break;
            }
         }
      }
      cachedNextMember = nextMember;
      cachedNextMemberTopology = cacheTopology.getTopologyId();
      return nextMember;
   }

   private Object handleReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      LocalizedCacheTopology cacheTopology = checkTopology(command);
      // SKIP_OWNERSHIP_CHECK is added when the entry is prefetched from remote node
      // TODO [rvansa]: local lookup and hinted read, see improvements in package-info

      // ClusteredGetCommand invokes local-mode forced read, but we still have to check for primary owner
      DistributionInfo info = cacheTopology.getDistribution(command.getKey());
      if (info.isPrimary()) {
         if (trace) {
            log.tracef("In topology %d this is primary owner", cacheTopology.getTopologyId());
         }
         return invokeNext(ctx, command);
      } else if (command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
         if (trace) {
            log.trace("Ignoring ownership");
         }
         return invokeNext(ctx, command);
      } else if (info.primary() == null) {
         throw OutdatedTopologyException.INSTANCE;
      } else if (ctx.isOriginLocal()) {
         if (isLocalModeForced(command) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)) {
            if (ctx.lookupEntry(command.getKey()) == null) {
               entryFactory.wrapExternalEntry(ctx, command.getKey(), NullCacheEntry.getInstance(), false, false);
            }
            return invokeNext(ctx, command);
         }
         ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(command.getKey(), command.getFlagsBitSet());
         clusteredGetCommand.setTopologyId(command.getTopologyId());
         CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(Collections.singletonList(info.primary()), clusteredGetCommand, syncIgnoreLeavers);
         Object key = clusteredGetCommand.getKey();
         return asyncInvokeNext(ctx, command, rpcFuture.thenAccept(responseMap -> {
            Response response = getSingleResponse(responseMap);
            if (response.isSuccessful()) {
               InternalCacheValue value = (InternalCacheValue) ((SuccessfulResponse) response).getResponseValue();
               if (value != null) {
                  InternalCacheEntry cacheEntry = value.toInternalCacheEntry(key);
                  entryFactory.wrapExternalEntry(ctx, key, cacheEntry, true, false);
               } else {
                  entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), false, false);
               }
            } else if (response instanceof UnsureResponse) {
               throw OutdatedTopologyException.INSTANCE;
            } else if (response instanceof CacheNotFoundResponse) {
               throw AllOwnersLostException.INSTANCE;
            } else {
               throw new IllegalArgumentException("Unexpected response " + response);
            }
         }));
      } else {
         return UnsureResponse.INSTANCE;
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      LocalizedCacheTopology cacheTopology = checkTopology(command);

      Map<Object, Object> originalMap = command.getMap();
      if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         extractAndSetMetadata(ctx, command, originalMap);
         return invokeNextThenAccept(ctx, command, putMapCommandHandler);
      }

      if (ctx.isOriginLocal()) {
         return invokeNextThenApply(ctx, command, (returnCtx, returnCommand, rv) ->
            handlePutMapOnOrigin(returnCtx, (PutMapCommand) returnCommand, rv, originalMap, cacheTopology));
      }

      // Remote
      if (command.isForwarded()) {
         // carries entries with version to back them up
         extractAndSetMetadata(ctx, command, originalMap);
         return invokeNextThenAccept(ctx, command, putMapCommandHandler);
      } else {
         // this node should be the primary
         Map<Object, VersionedResult> versionMap = new HashMap<>(originalMap.size());
         for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
            Object key = entry.getKey();
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null) {
               throw new IllegalStateException("Not wrapped " + key);
            }
            EntryVersion version = svm.incrementVersion(keyPartitioner.getSegment(key));
            cacheEntry.setMetadata(addVersion(command.getMetadata(), version));
            versionMap.put(key, new VersionedResult(null, version));
         }
         // disable ignore return values as this controls isReturnValueExpected with versionMap
         command.setFlagsBitSet(command.getFlagsBitSet() & ~FlagBitSets.IGNORE_RETURN_VALUES);
         return invokeNextThenApply(ctx, command, (ctx1, command1, rv) -> {
            for (Object key : ((PutMapCommand) command1).getAffectedKeys()) {
               commitSingleEntryIfNewer(ctx1.lookupEntry(key), ctx1, command1);
            }
            if (rv instanceof Map) {
               Map<?, Object> resultMap = (Map<?, Object>) rv;
               for (Map.Entry<?, Object> entry : resultMap.entrySet()) {
                  versionMap.compute(entry.getKey(), (k, vr) -> new VersionedResult(entry.getValue(), vr.version));
               }
            }
            return versionMap;
         });
      }
   }

   private Object handlePutMapOnOrigin(InvocationContext ctx, PutMapCommand command, Object rv, Map<Object, Object> originalMap, LocalizedCacheTopology cacheTopology) {
      if (!command.isSuccessful()) {
         return null;
      }

      Map<Object, CacheEntry> lookedUpEntries = ctx.getLookedUpEntries();
      Map<Address, Map<Object, Object>> remoteEntries = new HashMap<>();
      Map<Object, InternalCacheValue> localEntries = new HashMap<>();
      for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
         Object key = entry.getKey();
         DistributionInfo info = cacheTopology.getDistribution(key);
         if (info.isPrimary()) {
            CacheEntry ctxEntry = lookedUpEntries.get(key);
            if (ctxEntry == null) {
               throw new CacheException("Entry not looked up for " + key);
            }
            EntryVersion version = svm.incrementVersion(info.segmentId());
            Metadata metadata = new EmbeddedMetadata.Builder().version(version).build();
            ctxEntry.setMetadata(metadata);
            localEntries.put(key, new MetadataImmortalCacheValue(entry.getValue(), metadata));
            commitSingleEntryIfNewer(ctxEntry, ctx, command);
         } else if (info.primary() == null) {
            throw OutdatedTopologyException.INSTANCE;
         } else {
            Map<Object, Object> currentEntries = remoteEntries.computeIfAbsent(info.primary(), k -> new HashMap<>());
            currentEntries.put(key, entry.getValue());
         }
      }

      PutMapFuture allFuture = new PutMapFuture(command, remoteEntries.size(), (Map<Object, Object>) rv);
      if (!localEntries.isEmpty()) {
         Address backup = getNextMember(cacheTopology);
         if (backup != null) {
            allFuture.counter++;
            // note: we abuse PutMapCommand a bit as we need it to transport versions as well, and it can
            // carry only single Metadata instance.
            PutMapCommand backupCommand = cf.buildPutMapCommand(localEntries, command.getMetadata(), command.getFlagsBitSet());
            backupCommand.setForwarded(true);
            rpcManager.invokeRemotelyAsync(Collections.singleton(backup), backupCommand, defaultSyncOptions).whenComplete((r, t) -> {
               if (t != null) {
                  allFuture.completeExceptionally(t);
               } else {
                  synchronized (allFuture) {
                     if (--allFuture.counter == 0) {
                        allFuture.complete(allFuture.map);
                     }
                  }
                  for (Map.Entry<Object, InternalCacheValue> entry : localEntries.entrySet()) {
                     svm.scheduleKeyInvalidation(entry.getKey(), entry.getValue().getMetadata().version(), false);
                  }
               }
            });
         }
      }
      for (Map.Entry<Address, Map<Object, Object>> ownerEntry : remoteEntries.entrySet()) {
         Address owner = ownerEntry.getKey();
         PutMapCommand toPrimary = cf.buildPutMapCommand(ownerEntry.getValue(), command.getMetadata(), command.getFlagsBitSet());
         CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(
            Collections.singletonList(owner), toPrimary, defaultSyncOptions);
         rpcFuture.whenComplete((responseMap, t) -> {
            if (t != null) {
               allFuture.completeExceptionally(t);
               return;
            }
            SuccessfulResponse response = getSuccessfulResponseOrFail(responseMap, allFuture, null);
            if (response == null) {
               return;
            }
            Object responseValue = response.getResponseValue();
            if (!(responseValue instanceof Map)) {
               allFuture.completeExceptionally(new CacheException("Response from " + owner + ": expected Map<?, VersionedResult> but it is " + responseValue).fillInStackTrace());
               return;
            }
            Map<?, VersionedResult> versions = (Map<?, VersionedResult>) responseValue;
            synchronized (allFuture) {
               if (allFuture.isDone()) {
                  return;
               }
               for (Map.Entry<?, VersionedResult> entry : versions.entrySet()) {
                  // we will serve as the backup
                  entryFactory.wrapExternalEntry(ctx, entry.getKey(), null, false, true);
                  CacheEntry cacheEntry = ctx.lookupEntry(entry.getKey());
                  VersionedResult result = entry.getValue();
                  if (result.result != null) {
                     if (allFuture.map == null) {
                        allFuture.map = new HashMap<>();
                     }
                     allFuture.map.put(entry.getKey(), result.result);
                  }
                  Metadata metadata = addVersion(command.getMetadata(), result.version);
                  cacheEntry.setValue(originalMap.get(entry.getKey()));
                  cacheEntry.setMetadata(metadata);
                  // we don't care about setCreated() since backup owner should not fire listeners
                  cacheEntry.setChanged(true);
                  boolean committed = commitSingleEntryIfNewer(cacheEntry, ctx, command);
                  if (committed && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
                     svm.scheduleKeyInvalidation(entry.getKey(), result.version, false);
                  }
               }
               if (--allFuture.counter == 0) {
                  allFuture.complete(allFuture.map);
               }
            }
         });
      }
      return asyncValue(allFuture);
   }

   private static class PutMapFuture extends CompletableFuture<Map<Object, Object>> {
      private PutMapCommand command;
      private int counter;
      private Map<Object, Object> map;

      public PutMapFuture(PutMapCommand command, int counter, Map<Object, Object> map) {
         this.command = command;
         this.counter = counter;
         this.map = map;
      }

      @Override
      public synchronized boolean completeExceptionally(Throwable ex) {
         command.fail();
         return super.completeExceptionally(ex);
      }
   }

   protected void extractAndSetMetadata(InvocationContext ctx, PutMapCommand command, Map<Object, Object> originalMap) {
      Map<Object, Object> valueMap = new HashMap<>(originalMap.size());
      for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
         Object key = entry.getKey();
         CacheEntry cacheEntry = ctx.lookupEntry(key);
         if (cacheEntry == null) {
            // since this is executed on backup node (or by ST), the entry was not wrapped
            entryFactory.wrapExternalEntry(ctx, key, null, false, true);
            cacheEntry = ctx.lookupEntry(key);
         }
         InternalCacheValue value = (InternalCacheValue) entry.getValue();
         Metadata entryMetadata = command.getMetadata() == null ? value.getMetadata()
            : command.getMetadata().builder().version(value.getMetadata().version()).build();
         cacheEntry.setMetadata(entryMetadata);
         valueMap.put(key, value.getValue());
      }
      command.setMap(valueMap);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      LocalizedCacheTopology cacheTopology = checkTopology(command);
      // The SKIP_OWNERSHIP_CHECK is added when the entries are prefetches from remote node

      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
         return invokeNext(ctx, command);
      }

      if (ctx.isOriginLocal()) {
         Map<Address, List<Object>> remoteKeys = new HashMap<>();
         for (Object key : command.getKeys()) {
            DistributionInfo info = cacheTopology.getDistribution(key);
            if (info.primary() == null) {
               throw OutdatedTopologyException.INSTANCE;
            } else if (!info.isPrimary()) {
               remoteKeys.computeIfAbsent(info.primary(), k -> new ArrayList<>()).add(key);
            }
         }

         if (remoteKeys.isEmpty()) {
            return invokeNext(ctx, command);
         }
         ClusteredGetAllFuture sync = new ClusteredGetAllFuture(remoteKeys.size());
         for (Map.Entry<Address, List<Object>> remote : remoteKeys.entrySet()) {
            List<Object> keys = remote.getValue();
            ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(keys, command.getFlagsBitSet(), null);
            clusteredGetAllCommand.setTopologyId(command.getTopologyId());
            CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(
                  Collections.singleton(remote.getKey()), clusteredGetAllCommand, syncIgnoreLeavers);
            rpcFuture.whenComplete(((responseMap, throwable) -> handleGetAllResponse(responseMap, throwable, ctx, keys, sync)));
         }
         return asyncInvokeNext(ctx, command, sync);
      } else { // remote
         for (Object key : command.getKeys()) {
            if (ctx.lookupEntry(key) == null) {
               return UnsureResponse.INSTANCE;
            }
         }
         return invokeNext(ctx, command);
      }
   }

   private void handleGetAllResponse(Map<Address, Response> responseMap, Throwable throwable, InvocationContext ctx,
                                     List<?> keys, ClusteredGetAllFuture allFuture) {
      if (throwable != null) {
         allFuture.completeExceptionally(throwable);
         return;
      }
      // While upon lost owners in dist/repl mode we only return a map with less entries, in scattered mode
      // we need to retry the operation in next topology which should have the new primary owners assigned
      SuccessfulResponse response = getSuccessfulResponseOrFail(responseMap, allFuture,
            rsp -> allFuture.completeExceptionally(rsp instanceof UnsureResponse?
                  OutdatedTopologyException.INSTANCE : AllOwnersLostException.INSTANCE));
      if (response == null) {
         return;
      }
      Object responseValue = response.getResponseValue();
      if (!(responseValue instanceof InternalCacheValue[])) {
         allFuture.completeExceptionally(new IllegalStateException("Unexpected response value: " + responseValue));
         return;
      }
      InternalCacheValue[] values = (InternalCacheValue[]) responseValue;
      if (keys.size() != values.length) {
         allFuture.completeExceptionally(new CacheException("Request and response lengths differ: keys=" + keys + ", response=" + Arrays.toString(values)));
         return;
      }
      synchronized (allFuture) {
         if (allFuture.isDone()) {
            return;
         }
         for (int i = 0; i < values.length; ++i) {
            Object key = keys.get(i);
            InternalCacheValue value = values[i];
            CacheEntry entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
            entryFactory.wrapExternalEntry(ctx, key, entry, true, false);
         }
         if (--allFuture.counter == 0) {
            allFuture.complete(null);
         }
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // local mode clear will have unpredictable results
      svm.clearInvalidations();
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = isSynchronous(command) ? syncIgnoreLeavers : defaultAsyncOptions;
         return makeStage(asyncInvokeNext(ctx, command, rpcManager.invokeRemotelyAsync(null, command, rpcOptions)))
               .thenAccept(ctx, command, clearHandler);
      } else {
         return invokeNextThenAccept(ctx, command, clearHandler);
      }
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public final Object visitGetKeysInGroupCommand(InvocationContext ctx,
                                                                   GetKeysInGroupCommand command) throws Throwable {
      final Object groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      CompletableFuture<Void> future = rpcManager.invokeRemotelyAsync(
            Collections.singleton(groupManager.getPrimaryOwner(groupName)),
            command, defaultSyncOptions).thenAccept(responses -> {
         if (!responses.isEmpty()) {
            Response response = responses.values().iterator().next();
            if (response instanceof SuccessfulResponse) {
               //noinspection unchecked
               List<CacheEntry> cacheEntries =
                  (List<CacheEntry>) ((SuccessfulResponse) response).getResponseValue();
               for (CacheEntry entry : cacheEntries) {
                  entryFactory.wrapExternalEntry(ctx, entry.getKey(), entry, true, false);
               }
            }
         }
      });
      return asyncInvokeNext(ctx, command, future);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   protected class PrimaryResponseHandler extends AbstractVisitor implements InvocationSuccessFunction {
      private final Object responseValue;
      private CacheEntry cacheEntry;
      private Object returnValue;

      public PrimaryResponseHandler(Object responseValue) {
         this.responseValue = responseValue;
      }

      private <C extends DataWriteCommand & MetadataAwareCommand> Object handleDataWriteCommand(InvocationContext ctx, C command) {
         EntryVersion version;
         if (command.isReturnValueExpected()) {
            if (!(responseValue instanceof MetadataImmortalCacheValue)) {
               throw new CacheException("Expected MetadataImmortalCacheValue as response but it is " + responseValue);
            }
            MetadataImmortalCacheValue micv = (MetadataImmortalCacheValue) responseValue;
            version = micv.getMetadata().version();
            returnValue = micv.getValue();
         } else {
            if (!(responseValue instanceof EntryVersion)) {
               throw new CacheException("Expected EntryVersion as response but it is " + responseValue);
            }
            version = (EntryVersion) responseValue;
            returnValue = null;
         }
         Metadata metadata = addVersion(command.getMetadata(), version);

         // TODO: skip lookup by returning from entry factory directly
         entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
         cacheEntry = ctx.lookupEntry(command.getKey());
         cacheEntry.setMetadata(metadata);
         // Primary succeeded, so apply the value locally
         command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
         return invokeNextThenApply(ctx, command, this);
      }

      private <C extends DataWriteCommand & MetadataAwareCommand> Object handleComputeCommand(InvocationContext ctx, C command) throws Throwable {
         if (!(responseValue instanceof MetadataImmortalCacheValue)) {
            throw new CacheException("Expected MetadataImmortalCacheValue as response but it is " + responseValue);
         }
         MetadataImmortalCacheValue micv = (MetadataImmortalCacheValue) responseValue;
         InternalCacheEntry ice = micv.toInternalCacheEntry(command.getKey());
         returnValue = ice.getValue();

         // TODO: skip lookup by returning from entry factory directly
         entryFactory.wrapExternalEntry(ctx, command.getKey(), ice, true, true);
         cacheEntry = ctx.lookupEntry(command.getKey());
         cacheEntry.setChanged(true);
         return apply(ctx, command, null);
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         DataWriteCommand cmd = (DataWriteCommand) rCommand;
         // We don't care about the local value, as we use MATCH_ALWAYS on backup
         boolean committed = commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
         if (committed && !cmd.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            svm.scheduleKeyInvalidation(cmd.getKey(), cacheEntry.getMetadata().version(), cacheEntry.isRemoved());
         }
         return returnValue;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handleDataWriteCommand(ctx, command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return handleDataWriteCommand(ctx, command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         return handleDataWriteCommand(ctx, command);
      }

      @Override
      public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
         return handleComputeCommand(ctx, command);
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         return handleComputeCommand(ctx, command);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }
   }
}
