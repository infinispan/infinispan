package org.infinispan.interceptors.distribution;

import static org.infinispan.commands.VisitableCommand.LoadType.DONT_LOAD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

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
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ArrayCollector;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
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
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.InvocationSuccessAction;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.NotifyHelper;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingletonMapResponseCollector;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   @Inject protected ScatteredVersionManager<Object> svm;
   @Inject protected GroupManager groupManager;
   @Inject protected TimeService timeService;
   @Inject protected CacheNotifier cacheNotifier;
   @Inject protected FunctionalNotifier functionalNotifier;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected DistributionManager distributionManager;

   private volatile Address cachedNextMember;
   private volatile int cachedNextMemberTopology = -1;

   private final InvocationSuccessAction putMapCommandHandler = (rCtx, rCommand, rv) -> {
      PutMapCommand putMapCommand = (PutMapCommand) rCommand;
      for (Object key : putMapCommand.getAffectedKeys()) {
         commitSingleEntryIfNewer((RepeatableReadEntry) rCtx.lookupEntry(key), rCtx, rCommand);
         // this handler is called only for ST or when isOriginLocal() == false so we don't have to invalidate
      }
   };

   private final InvocationSuccessAction clearHandler = this::handleClear;

   private InvocationSuccessFunction handleWritePrimaryResponse = this::handleWritePrimaryResponse;
   private InvocationSuccessFunction handleWriteManyOnPrimary = this::handleWriteManyOnPrimary;

   private PutMapHelper putMapHelper = new PutMapHelper(helper -> null);
   private ReadWriteManyHelper readWriteManyHelper = new ReadWriteManyHelper(helper -> null);
   private ReadWriteManyEntriesHelper readWriteManyEntriesHelper = new ReadWriteManyEntriesHelper(helper -> null);
   private WriteOnlyManyHelper writeOnlyManyHelper = new WriteOnlyManyHelper(helper -> null);
   private WriteOnlyManyEntriesHelper writeOnlyManyEntriesHelper = new WriteOnlyManyEntriesHelper(helper -> null);

   private Object handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      RepeatableReadEntry cacheEntry = (RepeatableReadEntry) ctx.lookupEntry(command.getKey());
      EntryVersion seenVersion = getVersionOrNull(cacheEntry);
      LocalizedCacheTopology cacheTopology = checkTopology(command);

      DistributionInfo info = cacheTopology.getDistribution(command.getKey());
      if (info.primary() == null) {
         throw new OutdatedTopologyException(cacheTopology.getTopologyId() + 1);
      }

      if (isLocalModeForced(command)) {
         RepeatableReadEntry contextEntry = cacheEntry;
         if (cacheEntry == null) {
            entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
            contextEntry = (RepeatableReadEntry) ctx.lookupEntry(command.getKey());
         }
         EntryVersion nextVersion = null;
         if (command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            // we don't increment versions with state transfer
         } else if (info.isPrimary()) {
            if (cacheTopology.getTopologyId() == 0 && command instanceof MetadataAwareCommand) {
               // Preload does not use functional commands which are not metadata-aware
               Metadata metadata = ((MetadataAwareCommand) command).getMetadata();
               svm.updatePreloadedEntryVersion(metadata.version());
            } else {
               // let's allow local-mode writes on primary owner, preserving versions
               nextVersion = svm.incrementVersion(info.segmentId());
            }
         }
         return commitSingleEntryOnReturn(ctx, command, contextEntry, nextVersion);
      }

      if (ctx.isOriginLocal()) {
         if (info.isPrimary()) {
            Object seenValue = cacheEntry.getValue();
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) ->
               handleWriteOnOriginPrimary(rCtx, (DataWriteCommand) rCommand, rv, cacheEntry, seenValue, seenVersion, cacheTopology, info));
         } else { // not primary owner
            CompletionStage<Map<Address, Response>> rpcFuture = singleWriteOnRemotePrimary(info.primary(), command);
            return asyncValue(rpcFuture).thenApply(ctx, command, handleWritePrimaryResponse);
         }
      } else { // remote origin
         if (info.isPrimary()) {
            // TODO [ISPN-3918]: the previous value is unreliable as this could be second invocation
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               DataWriteCommand cmd = (DataWriteCommand) rCommand;
               if (!cmd.isSuccessful()) {
                  if (trace) log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", cmd);
                  singleWriteResponse(rCtx, cmd, rv);
                  return rv;
               }

               EntryVersion nextVersion = svm.incrementVersion(info.segmentId());
               Metadata metadata = addVersion(cacheEntry.getMetadata(), nextVersion);
               cacheEntry.setMetadata(metadata);

               if (cmd.loadType() != DONT_LOAD) {
                  commitSingleEntryIfNoChange(cacheEntry, rCtx, cmd);
               } else {
                  commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
               }

               Object returnValue = cmd.acceptVisitor(ctx, new PrimaryResponseGenerator(cacheEntry, rv));
               singleWriteResponse(rCtx, cmd, returnValue);
               return returnValue;
            });
         } else {
            // The origin is primary and we're merely backup saving the data
            assert cacheEntry == null || command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK);
            RepeatableReadEntry contextEntry;
            if (cacheEntry == null) {
               entryFactory.wrapExternalEntry(ctx, command.getKey(), null, false, true);
               contextEntry = (RepeatableReadEntry) ctx.lookupEntry(command.getKey());
            } else {
               contextEntry = cacheEntry;
            }
            return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
               commitSingleEntryIfNewer(contextEntry, rCtx, rCommand);
               return null;
            });
         }
      }
   }

   /**
    * This method is called by a non-owner sending write request to the primary owner
    */
   protected CompletionStage<Map<Address, Response>> singleWriteOnRemotePrimary(Address target, DataWriteCommand command) {
      return rpcManager.invokeCommand(target, command, SingletonMapResponseCollector.validOnly(), rpcManager.getSyncRpcOptions());
   }

   protected CompletionStage<Map<Address, Response>> manyWriteOnRemotePrimary(Address target, WriteCommand command) {
      return rpcManager.invokeCommand(target, command, SingletonMapResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions());
   }

   /**
    * This is a hook for bias-enabled mode where the primary performs additional RPCs but replication to another node.
    * The returned CF will be complete when both the provided <code>rpcFuture</code> completes and all additional RPCs
    * are complete, too. Failure in any of the RPCs will fail this future.
    */
   protected CompletionStage<?> completeSingleWriteOnPrimaryOriginator(DataWriteCommand command, Address backup, CompletionStage<?> rpcFuture) {
      return rpcFuture;
   }

   private Object handleWriteOnOriginPrimary(InvocationContext ctx, DataWriteCommand command, Object rv,
                                             RepeatableReadEntry cacheEntry, Object seenValue, EntryVersion seenVersion,
                                             CacheTopology cacheTopology, DistributionInfo info) {
      if (!command.isSuccessful()) {
         if (trace)
            log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
         return rv;
      }

      // increment the version
      EntryVersion nextVersion = svm.incrementVersion(info.segmentId());
      Metadata metadata = addVersion(cacheEntry.getMetadata(), nextVersion);
      cacheEntry.setMetadata(metadata);

      if (command.loadType() != DONT_LOAD) {
         commitSingleEntryIfNoChange(cacheEntry, ctx, command);
      } else {
         commitSingleEntryIfNewer(cacheEntry, ctx, command);
      }

      // When replicating to backup, we'll add skip ownership check since we're now on primary owner
      // and we have already committed the entry, reading the return value. If we got OTE from remote
      // site and the command would be retried, we could fail to do the retry/return wrong value.
      WriteCommand backupCommand;
      long flags = command.getFlagsBitSet() | FlagBitSets.SKIP_OWNERSHIP_CHECK;
      if (cacheEntry.isRemoved()) {
         backupCommand = cf.buildRemoveCommand(command.getKey(), null, flags);
         ((RemoveCommand) backupCommand).setMetadata(cacheEntry.getMetadata());
      } else {
         backupCommand = cf.buildPutKeyValueCommand(command.getKey(), cacheEntry.getValue(), cacheEntry.getMetadata(), flags);
      }
      backupCommand.setTopologyId(command.getTopologyId());
      Address backup = getNextMember(cacheTopology);
      if (backup != null) {
         // error responses throw exceptions from JGroupsTransport
         SingleResponseCollector collector = SingleResponseCollector.validOnly();
         CompletionStage<?> rpcFuture =
               rpcManager.invokeCommand(backup, backupCommand, collector, rpcManager.getSyncRpcOptions());
         rpcFuture.thenRun(() -> {
            if (cacheEntry.isCommitted() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               scheduleKeyInvalidation(command.getKey(), cacheEntry.getMetadata().version(), cacheEntry.isRemoved());
            }
         });
         rpcFuture = completeSingleWriteOnPrimaryOriginator(command, backup, rpcFuture);
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

   private Object commitSingleEntryOnReturn(InvocationContext ctx, DataWriteCommand command, RepeatableReadEntry cacheEntry,
                                            EntryVersion nextVersion) {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (nextVersion != null) {
            cacheEntry.setMetadata(addVersion(cacheEntry.getMetadata(), nextVersion));
         }
         if (command.loadType() != DONT_LOAD) {
            commitSingleEntryIfNoChange(cacheEntry, rCtx, rCommand);
         } else {
            commitSingleEntryIfNewer(cacheEntry, rCtx, dataWriteCommand);
         }
         if (cacheEntry.isCommitted() && rCtx.isOriginLocal() && nextVersion != null) {
            scheduleKeyInvalidation(dataWriteCommand.getKey(), nextVersion, cacheEntry.isRemoved());
         }
      });
   }

   protected void scheduleKeyInvalidation(Object key, EntryVersion nextVersion, boolean removed) {
      svm.scheduleKeyInvalidation(key, nextVersion, removed);
   }

   private void commitSingleEntryIfNewer(RepeatableReadEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
         }
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
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
               entry.setCommitted();
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         InequalVersionComparisonResult comparisonResult;
         if (oldMetadata == null || oldMetadata.version() == null || newMetadata == null || newMetadata.version() == null
            || (comparisonResult = oldMetadata.version().compareTo(newMetadata.version())) == InequalVersionComparisonResult.BEFORE
            || (oldMetadata instanceof RemoteMetadata && comparisonResult == InequalVersionComparisonResult.EQUAL)) {
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            entry.setCommitted();
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

      if (entry.isCommitted()) {
         NotifyHelper.entryCommitted(cacheNotifier, functionalNotifier, entry.isCreated(), entry.isRemoved(), entry.isExpired(),
               entry, ctx, (FlagAffectedCommand) command, entry.getOldValue(), entry.getOldMetadata());
      } // else we skip the notification, and the already executed notification skipped this (intermediate) update
   }

   private void commitSingleEntryIfNoChange(RepeatableReadEntry entry, InvocationContext ctx, VisitableCommand command) {
      if (!entry.isChanged()) {
         if (trace) {
            log.tracef("Entry has not changed, not committing");
         }
      }
      // RemoveCommand does not null the entry value
      if (entry.isRemoved()) {
         entry.setValue(null);
      }

      // We cannot delegate the dataContainer.compute() to entry.commit() as we need to reliably
      // retrieve previous value and metadata, but the entry API does not provide these.
      dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
         // newMetadata is null in case of local-mode write on non-primary owners
         Metadata newMetadata = entry.getMetadata();
         if (oldEntry == null) {
            if (entry.getOldValue() != null) {
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
               entry.setCommitted();
               return factory.create(entry);
            }
         }
         Metadata oldMetadata = oldEntry.getMetadata();
         EntryVersion oldVersion = oldMetadata == null ? null : oldMetadata.version();
         Metadata seenMetadata = entry.getOldMetadata();
         EntryVersion seenVersion = seenMetadata == null ? null : seenMetadata.version();
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
            if (trace) {
               log.tracef("Committing entry %s, replaced %s", entry, oldEntry);
            }
            entry.setCommitted();
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

      if (entry.isCommitted()) {
         NotifyHelper.entryCommitted(cacheNotifier, functionalNotifier, entry.isCreated(), entry.isRemoved(), entry.isExpired(),
               entry, ctx, (FlagAffectedCommand) command, entry.getOldValue(), entry.getOldMetadata());
      } // else we skip the notification, and the already executed notification skipped this (intermediate) update
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

      CacheEntry entry = ctx.lookupEntry(command.getKey());
      if (entry != null) {
         return invokeNext(ctx, command);
      }

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
         throw new OutdatedTopologyException(cacheTopology.getTopologyId() + 1);
      } else if (ctx.isOriginLocal()) {
         if (isLocalModeForced(command) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)) {
            entryFactory.wrapExternalEntry(ctx, command.getKey(), NullCacheEntry.getInstance(), false, false);
            return invokeNext(ctx, command);
         }
         ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(command.getKey(), command.getFlagsBitSet());
         clusteredGetCommand.setTopologyId(command.getTopologyId());
         SingletonMapResponseCollector collector = SingletonMapResponseCollector.ignoreLeavers();
         CompletionStage<Map<Address, Response>> rpcFuture =
               rpcManager.invokeCommand(info.primary(), clusteredGetCommand, collector, rpcManager.getSyncRpcOptions());
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
      if (command.isForwarded() || command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         assert command.getMetadata() == null || command.getMetadata().version() == null;

         Map<Object, Object> valueMap = new HashMap<>(command.getMap().size());
         for (Map.Entry<?, ?> entry : command.getMap().entrySet()) {
            Object key = entry.getKey();
            CacheEntry cacheEntry = ctx.lookupEntry(key);
            if (cacheEntry == null) {
               // since this is executed on backup node (or by ST), the entry was not wrapped
               entryFactory.wrapExternalEntry(ctx, key, null, false, true);
               cacheEntry = ctx.lookupEntry(key);
            }
            // TODO: we should set version only after the command has executed but as it won't modify version
            // on its own, we can do it right here
            InternalCacheValue value = (InternalCacheValue) entry.getValue();
            Metadata entryMetadata = command.getMetadata() == null ? value.getMetadata()
               : command.getMetadata().builder().version(value.getMetadata().version()).build();
            cacheEntry.setMetadata(entryMetadata);
            valueMap.put(key, value.getValue());
         }
         command.setMap(valueMap);
         return invokeNextThenAccept(ctx, command, putMapCommandHandler);
      } else {
         return handleWriteManyCommand(ctx, command, putMapHelper);
      }
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
            if (ctx.lookupEntry(key) != null) {
               continue;
            }
            DistributionInfo info = cacheTopology.getDistribution(key);
            if (info.primary() == null) {
               throw new OutdatedTopologyException(cacheTopology.getTopologyId() + 1);
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
            SingletonMapResponseCollector collector = SingletonMapResponseCollector.ignoreLeavers();
            CompletionStage<Map<Address, Response>> rpcFuture =
                  rpcManager.invokeCommand(remote.getKey(), clusteredGetAllCommand, collector,
                                           rpcManager.getSyncRpcOptions());
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
         if (isSynchronous(command)) {
            RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
            MapResponseCollector collector = MapResponseCollector.ignoreLeavers();
            return makeStage(
               asyncInvokeNext(ctx, command, rpcManager.invokeCommandOnAll(command, collector, rpcOptions)))
                  .thenAccept(ctx, command, clearHandler);
         } else {
            rpcManager.sendToAll(command, DeliverOrder.PER_SENDER);
            return invokeNextThenAccept(ctx, command, clearHandler);
         }
      } else {
         return invokeNextThenAccept(ctx, command, clearHandler);
      }
   }

   protected void handleClear(InvocationContext ctx, VisitableCommand command, Object ignored) {
      List<InternalCacheEntry<Object, Object>> copyEntries = new ArrayList<>(dataContainer.entrySet());
      dataContainer.clear();
      for (InternalCacheEntry entry : copyEntries) {
         cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, ctx, (ClearCommand) command);
      }
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry != null) {
         // the entry is owned locally (it is NullCacheEntry if it was not found), no need to go remote
         return invokeNext(ctx, command);
      }
      if (!ctx.isOriginLocal()) {
         return UnsureResponse.INSTANCE;
      }
      if (isLocalModeForced(command) || command.hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         if (ctx.lookupEntry(command.getKey()) == null) {
            entryFactory.wrapExternalEntry(ctx, command.getKey(), NullCacheEntry.getInstance(), false, false);
         }
         return invokeNext(ctx, command);
      }
      DistributionInfo info = checkTopology(command).getDistribution(command.getKey());
      if (info.primary() == null) {
         throw AllOwnersLostException.INSTANCE;
      }
      SingletonMapResponseCollector collector = SingletonMapResponseCollector.ignoreLeavers();
      CompletionStage<Map<Address, Response>> rpc = rpcManager.invokeCommand(info.primary(), command, collector, rpcManager.getSyncRpcOptions());
      return asyncValue(rpc.thenApply(responses -> {
         Response response = getSingleResponse(responses);
         if (response.isSuccessful()) {
            return ((SuccessfulResponse) response).getResponseValue();
         } else if (response instanceof UnsureResponse) {
            throw OutdatedTopologyException.INSTANCE;
         } else if (response instanceof CacheNotFoundResponse) {
            throw AllOwnersLostException.INSTANCE;
         } else {
            throw new IllegalArgumentException("Unexpected response " + response);
         }
      }));
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         return handleLocalOnlyReadManyCommand(ctx, command, command.getKeys());
      }

      LocalizedCacheTopology cacheTopology = checkTopology(command);
      if (!ctx.isOriginLocal()) {
         return handleRemoteReadManyCommand(ctx, command, command.getKeys());
      }
      if (command.getKeys().isEmpty()) {
         return Stream.empty();
      }

      ConsistentHash ch = cacheTopology.getReadConsistentHash();
      int estimateForOneNode = 2 * command.getKeys().size() / ch.getMembers().size();
      Function<Address, List<Object>> createList = k -> new ArrayList<>(estimateForOneNode);
      Map<Address, List<Object>> requestedKeys = new HashMap<>();
      List<Object> localKeys = null;
      for (Object key : command.getKeys()) {
         if (ctx.lookupEntry(key) != null) {
            if (localKeys == null) {
               localKeys = new ArrayList<>();
            }
            localKeys.add(key);
            continue;
         }
         DistributionInfo info = cacheTopology.getDistribution(key);
         assert !info.isPrimary();
         if (info.primary() == null) {
            throw AllOwnersLostException.INSTANCE;
         }
         requestedKeys.computeIfAbsent(info.primary(), createList).add(key);
      }

      MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(
            requestedKeys.size() + (localKeys == null ? 0 : 1), new Object[command.getKeys().size()], Arrays::stream);

      int offset = 0;
      if (localKeys != null) {
         offset += localKeys.size();
         ReadOnlyManyCommand localCommand = new ReadOnlyManyCommand(command).withKeys(localKeys);
         invokeNextAndFinally(ctx, localCommand, (rCtx, rCommand, rv, throwable) -> {
            if (throwable != null) {
               allFuture.completeExceptionally(throwable);
            } else {
               try {
                  ((Stream) rv).collect(new ArrayCollector(allFuture.results));
                  allFuture.countDown();
               } catch (Throwable t) {
                  allFuture.completeExceptionally(t);
               }
            }
         });
      }

      for (Map.Entry<Address, List<Object>> addressKeys : requestedKeys.entrySet()) {
         List<Object> keysForAddress = addressKeys.getValue();
         ReadOnlyManyCommand remoteCommand = new ReadOnlyManyCommand(command).withKeys(keysForAddress);
         remoteCommand.setTopologyId(command.getTopologyId());
         Set<Address> target = Collections.singleton(addressKeys.getKey());
         int myOffset = offset;
         SingletonMapResponseCollector collector = SingletonMapResponseCollector.ignoreLeavers();
         CompletionStage<Map<Address, Response>> rpc =
               rpcManager.invokeCommand(target, remoteCommand, collector, rpcManager.getSyncRpcOptions());
         rpc.whenComplete((responseMap, throwable) -> {
                  if (throwable != null) {
                     allFuture.completeExceptionally(throwable);
                     return;
                  }
                  SuccessfulResponse response = getSuccessfulResponseOrFail(responseMap, allFuture,
                        rsp -> allFuture.completeExceptionally(rsp instanceof UnsureResponse?
                        OutdatedTopologyException.INSTANCE : AllOwnersLostException.INSTANCE));
                  if (response == null) {
                     return;
                  }
                  try {
                     Object[] values = (Object[]) response.getResponseValue();
                     if (values != null) {
                        System.arraycopy(values, 0, allFuture.results, myOffset, values.length);
                        allFuture.countDown();
                     } else {
                        allFuture.completeExceptionally(new IllegalStateException("Unexpected response value " + response.getResponseValue()));
                     }
                  } catch (Throwable t) {
                     allFuture.completeExceptionally(t);
                  }
               });
         offset += keysForAddress.size();
      }
      return asyncValue(allFuture);
   }

   private Object handleLocalOnlyReadManyCommand(InvocationContext ctx, VisitableCommand command, Collection<?> keys) {
      for (Object key : keys) {
         if (ctx.lookupEntry(key) == null) {
            entryFactory.wrapExternalEntry(ctx, key, NullCacheEntry.getInstance(), true, false);
         }
      }
      return invokeNext(ctx, command);
   }

   private <C extends TopologyAffectedCommand & VisitableCommand> Object handleRemoteReadManyCommand(
         InvocationContext ctx, C command, Collection<?> keys) {
      for (Object key : keys) {
         if (ctx.lookupEntry(key) == null) {
            return UnsureResponse.INSTANCE;
         }
      }
      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> ((Stream) rv).toArray());
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   private <C extends WriteCommand, Container, Item> Object handleWriteManyCommand(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper) {
      if (ctx.isOriginLocal()) {
         return handleWriteManyOnOrigin(ctx, command, helper);
      } else {
         checkTopology(command);
         // Functional commands cannot be forwarded, because we use PutMapCommand for backup
         // this node should be the primary
         assert helper.shouldRegisterRemoteCallback(command);
         return invokeNextThenApply(ctx, command, handleWriteManyOnPrimary);
      }
   }

   private <C extends WriteCommand, Container, Item> Object handleWriteManyOnOrigin(
         InvocationContext ctx, C command, WriteManyCommandHelper<C, Container, Item> helper) {
      LocalizedCacheTopology cacheTopology = checkTopology(command);

      Map<Address, Container> remoteEntries = new HashMap<>();
      for (Item item : helper.getItems(command)) {
         Object key = helper.item2key(item);
         DistributionInfo info = cacheTopology.getDistribution(key);
         Address primary = info.primary();
         if (primary == null) {
            throw AllOwnersLostException.INSTANCE;
         } else {
            Container currentEntries = remoteEntries.computeIfAbsent(primary, k -> helper.newContainer());
            helper.accumulate(currentEntries, item);
         }
      }

      Object[] results = command.loadType() == DONT_LOAD ? null : new Object[command.getAffectedKeys().size()];
      MergingCompletableFuture<Object> allFuture = new SyncMergingCompletableFuture<>(remoteEntries.size(), results, helper::transformResult);

      int offset = 0;
      Container localEntries = remoteEntries.remove(rpcManager.getAddress());
      if (localEntries != null) {
         helper.containerSize(localEntries);
         C localCommand = helper.copyForLocal(command, localEntries);
         localCommand.setTopologyId(command.getTopologyId());
         LocalWriteManyHandler handler = new LocalWriteManyHandler(allFuture, localCommand.getAffectedKeys(), cacheTopology);
         invokeNextAndFinally(ctx, localCommand, handler);
      }

      for (Map.Entry<Address, Container> ownerEntry : remoteEntries.entrySet()) {
         Address owner = ownerEntry.getKey();
         // TODO: copyForLocal just creates the command with given entries, not using the segment-aware map
         Container container = ownerEntry.getValue();
         C toPrimary = helper.copyForLocal(command, container);
         toPrimary.setTopologyId(command.getTopologyId());
         CompletionStage<Map<Address, Response>> rpcFuture = manyWriteOnRemotePrimary(owner, toPrimary);
         int myOffset = offset;
         offset += helper.containerSize(container);
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
            // Note: we could use PrimaryResponseHandler, but we would have to add the reference to allFuture, offset...
            InternalCacheValue[] values;
            try {
               if (command.loadType() == DONT_LOAD) {
                  if (!(responseValue instanceof InternalCacheValue[])) {
                     allFuture.completeExceptionally(new CacheException("Response from " + owner + ": expected InternalCacheValue[] but it is " + responseValue));
                     return;
                  }
                  values = (InternalCacheValue[]) responseValue;
               } else {
                  if (!(responseValue instanceof Object[]) || (((Object[]) responseValue).length != 2)) {
                     allFuture.completeExceptionally(new CacheException("Response from " + owner + ": expected Object[2] but it is " + responseValue));
                     return;
                  }
                  // We use Object[] { InternalCacheValue[], Object[] } structure to get benefit of same-type array marshalling
                  // TODO optimize returning entry itself
                  // Note: some interceptors relying on the return value *could* have a problem interpreting this
                  values = (InternalCacheValue[]) ((Object[]) responseValue)[0];
                  MergingCompletableFuture.moveListItemsToFuture(((Object[]) responseValue)[1], allFuture, myOffset);
               }
               synchronized (allFuture) {
                  if (allFuture.isDone()) {
                     return;
                  }
                  int i = 0;
                  for (Object key : helper.toKeys(container)) {
                     // we will serve as the backup
                     InternalCacheEntry ice = values[i++].toInternalCacheEntry(key);
                     entryFactory.wrapExternalEntry(ctx, key, ice, true, true);
                     RepeatableReadEntry entry = (RepeatableReadEntry) ctx.lookupEntry(key);
                     // we don't care about setCreated() since backup owner should not fire listeners
                     entry.setChanged(true);
                     commitSingleEntryIfNewer(entry, ctx, command);
                     if (entry.isCommitted() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
                        scheduleKeyInvalidation(entry.getKey(), entry.getMetadata().version(), entry.isRemoved());
                     }
                  }
                  assert i == values.length;
               }
               allFuture.countDown();
            } catch (Throwable t2) {
               allFuture.completeExceptionally(t2);
            }
         });
      }
      return asyncValue(allFuture);
   }

   private Object handleWriteManyOnPrimary(InvocationContext ctx, VisitableCommand command, Object rv) {
      WriteCommand cmd = (WriteCommand) command;
      int numKeys = cmd.getAffectedKeys().size();
      InternalCacheValue[] values = new InternalCacheValue[numKeys];
      // keys are always iterated in order
      int i = 0;
      for (Object key : cmd.getAffectedKeys()) {
         RepeatableReadEntry entry = (RepeatableReadEntry) ctx.lookupEntry(key);
         EntryVersion nextVersion = svm.incrementVersion(keyPartitioner.getSegment(key));
         entry.setMetadata(addVersion(entry.getMetadata(), nextVersion));
         if (cmd.loadType() == DONT_LOAD) {
            commitSingleEntryIfNewer(entry, ctx, command);
         } else {
            commitSingleEntryIfNoChange(entry, ctx, command);
         }
         values[i++] = new MetadataImmortalCacheValue(entry.getValue(), entry.getMetadata());
      }
      if (cmd.loadType() == DONT_LOAD) {
         // Disable ignoring return value in response
         cmd.setFlagsBitSet(cmd.getFlagsBitSet() & ~FlagBitSets.IGNORE_RETURN_VALUES);
         manyWriteResponse(ctx, cmd, values);
         return values;
      } else {
         Object[] returnValue = {values, ((List) rv).toArray()};
         manyWriteResponse(ctx, cmd, returnValue);
         return returnValue;
      }
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, writeOnlyManyEntriesHelper);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, writeOnlyManyHelper);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, readWriteManyHelper);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleWriteManyCommand(ctx, command, readWriteManyEntriesHelper);
   }

   @Override
   public final Object visitGetKeysInGroupCommand(InvocationContext ctx,
                                                                   GetKeysInGroupCommand command) throws Throwable {
      final Object groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      Address primary = distributionManager.getCacheTopology().getDistribution(groupName).primary();
      CompletionStage<Void> future =
            rpcManager.invokeCommand(primary, command, SingleResponseCollector.validOnly(),
                                     rpcManager.getSyncRpcOptions())
                  .thenAccept(response -> {
                     if (response instanceof SuccessfulResponse) {
                        //noinspection unchecked
                        List<CacheEntry> cacheEntries =
                              (List<CacheEntry>) response.getResponseValue();
                        for (CacheEntry entry : cacheEntries) {
                           entryFactory.wrapExternalEntry(ctx, entry.getKey(), entry, true, false);
                        }
                     }
                  });
      return asyncInvokeNext(ctx, command, future);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   protected static class PrimaryResponseGenerator extends AbstractVisitor {
      private final CacheEntry cacheEntry;
      private final Object returnValue;

      public PrimaryResponseGenerator(CacheEntry cacheEntry, Object rv) {
         this.cacheEntry = cacheEntry;
         this.returnValue = rv;
      }

      private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand cmd) {
         if (cmd.isReturnValueExpected()) {
            return new Object[] { returnValue, cacheEntry.getMetadata().version() };
         } else {
            // force return value to be sent in the response (the version)
            cmd.setFlagsBitSet(cmd.getFlagsBitSet() & ~(FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.SKIP_REMOTE_LOOKUP));
            return cacheEntry.getMetadata().version();
         }
      }

      private Object handleValueResponseCommand(InvocationContext ctx, DataWriteCommand cmd) {
         return new MetadataImmortalCacheValue(cacheEntry.getValue(), cacheEntry.getMetadata());
      }

      private Object handleFunctionalCommand(InvocationContext ctx, DataWriteCommand cmd) {
         return new Object[] { cacheEntry.getValue(), cacheEntry.getMetadata(), returnValue };
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
         // This is actually one place where existence of ComputeCommand (as opposed to basing compute() method
         // on top of ReadWriteKeyCommand) gives some advantage: we know that stored entry is equal to command's return
         // value so we don't have to send it twice.
         // TODO: optimize case where new value == return value in RKWC
         return handleValueResponseCommand(ctx, command);
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         return handleValueResponseCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
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

   protected class PrimaryResponseHandler extends AbstractVisitor implements InvocationSuccessFunction {
      private final Object responseValue;
      private Object returnValue;
      private EntryVersion version;

      public PrimaryResponseHandler(Object responseValue) {
         this.responseValue = responseValue;
      }

      private Object handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
         if (command.isReturnValueExpected()) {
            if (!(responseValue instanceof Object[])) {
               throw new CacheException("Expected Object[] { return-value, version } as response but it is " + responseValue);
            }
            Object[] array = (Object[]) this.responseValue;
            if (array.length != 2) {
               throw new CacheException("Expected Object[] { return-value, version } but it is " + Arrays.toString(array));
            }
            version = (EntryVersion) array[1];
            returnValue = array[0];
         } else {
            if (!(responseValue instanceof EntryVersion)) {
               throw new CacheException("Expected EntryVersion as response but it is " + responseValue);
            }
            version = (EntryVersion) responseValue;
            returnValue = null;
         }
         entryFactory.wrapExternalEntry(ctx, command.getKey(), NullCacheEntry.getInstance(), false, true);
         // Primary succeeded, so apply the value locally
         command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
         return invokeNextThenApply(ctx, command, this);
      }

      private Object handleValueResponseCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
         if (!(responseValue instanceof MetadataImmortalCacheValue)) {
            throw new CacheException("Expected MetadataImmortalCacheValue as response but it is " + responseValue);
         }
         MetadataImmortalCacheValue micv = (MetadataImmortalCacheValue) responseValue;
         InternalCacheEntry ice = micv.toInternalCacheEntry(command.getKey());
         returnValue = ice.getValue();
         version = ice.getMetadata().version();

         entryFactory.wrapExternalEntry(ctx, command.getKey(), ice, true, true);
         return apply(ctx, command, null);
      }

      private Object handleFunctionalCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
         if (!(responseValue instanceof Object[])) {
            throw new CacheException("Expected Object[] { value, metadata, return-value } but it is " + responseValue);
         }
         Object[] array = (Object[]) responseValue;
         if (array.length != 3) {
            throw new CacheException("Expected Object[] { value, metadata, return-value } but it is " + Arrays.toString(array));
         }
         Metadata metadata = (Metadata) array[1];
         returnValue = array[2];
         version = metadata.version();

         entryFactory.wrapExternalEntry(ctx, command.getKey(), new MetadataImmortalCacheEntry(command.getKey(), array[0], metadata), true, true);
         return apply(ctx, command, null);
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         DataWriteCommand cmd = (DataWriteCommand) rCommand;

         RepeatableReadEntry cacheEntry = (RepeatableReadEntry) rCtx.lookupEntry(cmd.getKey());
         Metadata metadata = addVersion(cacheEntry.getMetadata(), version);
         cacheEntry.setMetadata(metadata);
         cacheEntry.setChanged(true);

         // We don't care about the local value, as we use MATCH_ALWAYS on backup
         commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
         if (cacheEntry.isCommitted() && !cmd.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            scheduleKeyInvalidation(cmd.getKey(), cacheEntry.getMetadata().version(), cacheEntry.isRemoved());
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
         return handleValueResponseCommand(ctx, command);
      }

      @Override
      public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
         return handleValueResponseCommand(ctx, command);
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         throw new UnsupportedOperationException();
      }

      @Override
      public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
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
      public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }

      @Override
      public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
         return handleFunctionalCommand(ctx, command);
      }
   }

   /**
    * This class uses synchronized {@link #completeExceptionally(Throwable)} for the same reasons as
    * {@link org.infinispan.interceptors.impl.ClusteringInterceptor.ClusteredGetAllFuture}
    */
   private static class SyncMergingCompletableFuture<T> extends MergingCompletableFuture<T> {
      SyncMergingCompletableFuture(int participants, T[] results, Function<T[], Object> transform) {
         super(participants, results, transform);
      }

      @Override
      public synchronized boolean completeExceptionally(Throwable ex) {
         return super.completeExceptionally(ex);
      }
   }

   private class LocalWriteManyHandler implements InvocationFinallyAction {
      private final MergingCompletableFuture allFuture;
      private final Collection<?> keys;
      private final LocalizedCacheTopology cacheTopology;

      private LocalWriteManyHandler(MergingCompletableFuture allFuture, Collection<?> keys, LocalizedCacheTopology cacheTopology) {
         this.allFuture = allFuture;
         this.keys = keys;
         this.cacheTopology = cacheTopology;
      }

      @Override
      public void accept(InvocationContext ctx, VisitableCommand command, Object rv, Throwable throwable) throws Throwable {
         if (throwable != null) {
            allFuture.completeExceptionally(throwable);
         } else try {
            if (allFuture.results != null) {
               MergingCompletableFuture.moveListItemsToFuture(rv, allFuture, 0);
            }
            WriteCommand writeCommand = (WriteCommand) command;
            Map<Object, InternalCacheValue> backupMap = new HashMap<>();
            synchronized (allFuture) {
               if (allFuture.isDone()) {
                  return;
               }
               for (Object key : keys) {
                  DistributionInfo info = cacheTopology.getDistribution(key);
                  EntryVersion version = svm.incrementVersion(info.segmentId());
                  RepeatableReadEntry entry = (RepeatableReadEntry) ctx.lookupEntry(key);
                  if (entry == null) {
                     throw new CacheException("Entry not looked up for " + key);
                  }
                  Metadata metadata = addVersion(entry.getMetadata(), version);
                  entry.setMetadata(metadata);
                  backupMap.put(key, new MetadataImmortalCacheValue(entry.getValue(), metadata));
                  if (writeCommand.loadType() == DONT_LOAD) {
                     commitSingleEntryIfNewer(entry, ctx, command);
                  } else {
                     commitSingleEntryIfNoChange(entry, ctx, command);
                  }
               }
            }
            Address backup = getNextMember(cacheTopology);
            completeManyWriteOnPrimaryOriginator(writeCommand, backup, allFuture);
            PutMapCommand backupCommand = cf.buildPutMapCommand(backupMap, null, writeCommand.getFlagsBitSet());
            backupCommand.setForwarded(true);
            backupCommand.setTopologyId(writeCommand.getTopologyId());
            ResponseCollector<Map<Address, Response>> collector = SingletonMapResponseCollector.ignoreLeavers();
            CompletionStage<Map<Address, Response>> rpcFuture =
                  rpcManager.invokeCommand(backup, backupCommand, collector, rpcManager.getSyncRpcOptions());
            rpcFuture.whenComplete((responseMap, throwable1) -> {
                     if (throwable1 != null) {
                        allFuture.completeExceptionally(throwable1);
                     } else {
                        allFuture.countDown();
                        for (Map.Entry<Object, Object> entry : backupCommand.getMap().entrySet()) {
                           EntryVersion version = ((InternalCacheValue) entry.getValue()).getMetadata().version();
                           scheduleKeyInvalidation(entry.getKey(), version, false);
                        }
                     }
            });
         } catch (Throwable t) {
            allFuture.completeExceptionally(t);
         }
      }
   }

   /**
    * This method is called by primary owner responding to the originator after write has been completed
    */
   protected void singleWriteResponse(InvocationContext ctx, DataWriteCommand cmd, Object returnValue) {
      // noop, just hook
   }

   protected void manyWriteResponse(InvocationContext ctx, WriteCommand cmd, Object returnValue) {
      // noop, just hook
   }

   /**
    * This is a hook for bias-enabled mode where the primary performs additional RPCs but replication to another node.
    * Implementation is expected to increment <code>future</code> on each additional RPC and decrement
    * it when the response arrives.
    */
   protected void completeManyWriteOnPrimaryOriginator(WriteCommand command, Address backup, CountDownCompletableFuture future) {
      // noop, just hook
   }
}
