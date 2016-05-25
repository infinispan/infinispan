package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.*;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.*;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.ByRef;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ScatteringInterceptor extends ClusteringInterceptor {
   private final static Log log = LogFactory.getLog(ScatteringInterceptor.class);
   private final static boolean trace = log.isTraceEnabled();

   protected ClusteringDependentLogic cdl;
   protected ScatteredVersionManager svm;
   protected GroupManager groupManager;
   protected TimeService timeService;
   private volatile Address cachedNextMember;
   private volatile int cachedNextMemberTopology = -1;
   private RpcOptions defaultSyncOptions;
   private RpcOptions getRpcOptions;

   private final ReturnHandler dataWriteCommandNoReadHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable != null) {
         throw throwable;
      }
      DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
      CacheEntry entry = rCtx.lookupEntry(dataWriteCommand.getKey());
      boolean committed = commitSingleEntryIfNewer(entry, rCtx, dataWriteCommand);
      if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
         svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), ((NumericVersion) entry.getMetadata().version()).getVersion(), entry.isRemoved());
      }
      return null;
   };
   private final ReturnHandler putMapCommandHandler = (rCtx, rCommand, rv, throwable) -> {
      if (throwable != null) {
         throw throwable;
      }
      for (Object key : ((PutMapCommand) rCommand).getAffectedKeys()) {
         commitSingleEntryIfNewer(rCtx.lookupEntry(key), rCtx, rCommand);
         // this handler is called only for ST or when isOriginLocal() == false so we don't have to invalidate
      }
      return null;
   };
   ;

   @Inject
   public void injectDependencies(GroupManager groupManager, ClusteringDependentLogic cdl, ScatteredVersionManager svm, TimeService timeService) {
      this.groupManager = groupManager;
      this.cdl = cdl;
      this.svm = svm;
      this.timeService = timeService;
   }

   @Start
   public void start() {
      defaultSyncOptions = rpcManager.getDefaultRpcOptions(true);
      getRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, DeliverOrder.NONE).build();
   }

   private CompletableFuture<Void> handleWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      CacheEntry cacheEntry = ctx.lookupEntry(command.getKey());
      NumericVersion seenVersion = getVersionOrNull(cacheEntry);

      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      if (cacheTopology == null) {
         // what else can we do before we get the first topology?
         // This happens during preload
         cacheEntry.setMetadata(command.getMetadata());
         return commitSingleEntryOnReturn(ctx, command, cacheEntry, seenVersion, cacheEntry.getValue());
      }
      checkTopology(command, cacheTopology);

      // lookup the segment to save repeated hashcode computations
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      int segment = ch.getSegment(command.getKey());
      Address address = cdl.getAddress();
      Address primaryOwner = ch.locatePrimaryOwnerForSegment(segment);
      if (primaryOwner == null) {
         throw new MissingOwnerException(cacheTopology.getTopologyId());
      }

      if (isLocalModeForced(command)) {
         if (command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
            cacheEntry.setMetadata(command.getMetadata());
         } else if (primaryOwner.equals(address)) {
            // let's allow local-mode writes on primary owner, preserving versions
            long nextVersion = svm.incrementVersion(segment);
            cacheEntry.setMetadata(addVersion(command.getMetadata(), new NumericVersion(nextVersion)));
         }
         return commitSingleEntryOnReturn(ctx, command, cacheEntry, seenVersion, cacheEntry.getValue());
      }

      if (ctx.isOriginLocal()) {
         command.addFlag(Flag.SKIP_LOCKING); // TODO: remove locking in handler
         if (primaryOwner.equals(address)) {
            Object prevValue = cacheEntry.getValue();
            return ctx.onReturn((ctx1, command1, localResult, throwable) -> {
               DataWriteCommand cmd = (DataWriteCommand) command1;
               if (!cmd.isSuccessful()) {
                  if (trace)
                     log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
                  return null; // propagates local result
               }

               // increment the version
               long nextVersion = svm.incrementVersion(segment);
               Metadata metadata = addVersion(cmd.getMetadata(), new NumericVersion(nextVersion));
               cacheEntry.setMetadata(metadata);
               cmd.setMetadata(metadata);

               boolean committed;
               if (cmd.readsExistingValues()) {
                  committed = commitSingleEntryIfNoChange(prevValue, seenVersion, cacheEntry, ctx1, command1);
               } else {
                  committed = commitSingleEntryIfNewer(cacheEntry, ctx1, command1);
               }

               cmd.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
               // When replicating to backup, we'll add skip ownership check since we're now on primary owner
               // and we have already committed the entry, reading the return value. If we got OTE from remote
               // site and the command would be retried, we could fail to do the retry/return wrong value.
               cmd.addFlag(Flag.SKIP_OWNERSHIP_CHECK);
               // TODO: maybe we should rather create a copy of the command with modifications...
               Address backup = getNextMember(cacheTopology, address);
               if (backup != null) {
                  // error responses throw exceptions from JGroupsTransport
                  CompletableFuture<Map<Address, Response>> rpcFuture =
                     rpcManager.invokeRemotelyAsync(Collections.singletonList(backup), cmd, defaultSyncOptions);
                  return rpcFuture.thenApply(responseMap -> {
                     if (committed && !cmd.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
                        svm.scheduleKeyInvalidation(cmd.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
                     }
                     return localResult;
                  });
               } else {
                  return null; // propagates local result
               }
            });
         } else { // not primary owner
            CompletableFuture<Map<Address, Response>> rpcFuture =
               rpcManager.invokeRemotelyAsync(Collections.singletonList(primaryOwner), command, defaultSyncOptions);
            return rpcFuture.thenAccept(responseMap -> {
               Response response = getSingleResponse(responseMap, command.getTopologyId());
               if (response.isSuccessful()) {
                  Object responseValue = ((SuccessfulResponse) response).getResponseValue();
                  NumericVersion version;
                  Object value;
                  if (command.isReturnValueExpected()) {
                     if (!(responseValue instanceof MetadataImmortalCacheValue)) {
                        throw new CacheException("Expected MetadataImmortalCacheValue as response but it is " + responseValue);
                     }
                     MetadataImmortalCacheValue micv = (MetadataImmortalCacheValue) responseValue;
                     version = (NumericVersion) micv.getMetadata().version();
                     value = micv.getValue();
                  } else {
                     if (!(responseValue instanceof NumericVersion)) {
                        throw new CacheException("Expected NumericVersion as response but it is " + responseValue);
                     }
                     version = (NumericVersion) responseValue;
                     value = null;
                  }
                  Metadata metadata = addVersion(command.getMetadata(), version);
                  cacheEntry.setMetadata(metadata);
                  // Primary succeeded, so apply the value locally
                  command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
                  ctx.onReturn((ctx1, command1, rv, throwable1) -> {
                     if (throwable1 != null) {
                        throw throwable1;
                     }
                     DataWriteCommand cmd = (DataWriteCommand) command1;
                     // We don't care about the local value, as we use MATCH_ALWAYS on backup
                     boolean committed = commitSingleEntryIfNewer(cacheEntry, ctx1, cmd);
                     if (committed && !cmd.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
                        svm.scheduleKeyInvalidation(cmd.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
                     }
                     return CompletableFuture.completedFuture(value);
                  });
               } else {
                  command.fail();
                  ctx.shortCircuit(((UnsuccessfulResponse) response).getResponseValue());
               }
            });
         }
      } else { // remote origin
         if (primaryOwner.equals(address)) {
            Object seenValue = cacheEntry.getValue();
            // TODO: the previous value is unreliable as this could be second invocation
            return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
               if (throwable != null) {
                  throw throwable;
               }
               DataWriteCommand cmd = (DataWriteCommand) rCommand;
               if (!cmd.isSuccessful()) {
                  if (trace) log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", cmd);
                  return CompletableFuture.completedFuture(cmd.isReturnValueExpected() ?
                     UnsuccessfulResponse.create(rv) : UnsuccessfulResponse.UNSUCCESSFUL_EMPTY_RESPONSE);
               }

               long nextVersion = svm.incrementVersion(segment);
               Metadata metadata = addVersion(cmd.getMetadata(), new NumericVersion(nextVersion));
               cacheEntry.setMetadata(metadata);
               cmd.setMetadata(metadata);

               if (cmd.readsExistingValues()) {
                  commitSingleEntryIfNoChange(seenValue, seenVersion, cacheEntry, rCtx, cmd);
               } else {
                  commitSingleEntryIfNewer(cacheEntry, rCtx, cmd);
               }

               if (cmd.isReturnValueExpected()) {
                  return CompletableFuture.completedFuture(SuccessfulResponse.create(new MetadataImmortalCacheValue(rv, metadata)));
               } else {
                  return CompletableFuture.completedFuture(SuccessfulResponse.create(metadata.version()));
               }
            });
         } else {
            // The origin is primary and we're merely backup saving the data
            cacheEntry.setMetadata(command.getMetadata());
            return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
               if (throwable != null) {
                  throw throwable;
               }
               commitSingleEntryIfNewer(cacheEntry, rCtx, rCommand);
               return null;
            });
         }
      }
   }

   private void checkTopology(FlagAffectedCommand command, CacheTopology cacheTopology) {
      if (!command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK) && command.getTopologyId() >= 0 && command.getTopologyId() != cacheTopology.getTopologyId()) {
         throw new OutdatedTopologyException("Command topology: " + command.getTopologyId() + ", cache topology: " + cacheTopology.getTopologyId());
      } else if (trace) {
         log.tracef("%s has topology %d (current is %d)", command, command.getTopologyId(), cacheTopology.getTopologyId());
      }
   }

   private CompletableFuture<Void> commitSingleEntryOnReturn(InvocationContext ctx, DataWriteCommand command, CacheEntry cacheEntry, NumericVersion prevVersion, Object prevValue) {
      if (command.readsExistingValues()) {
         return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
            if (throwable != null) {
               throw throwable;
            }
            DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
            boolean committed = commitSingleEntryIfNoChange(prevValue, prevVersion, cacheEntry, rCtx, rCommand);
            if (committed && rCtx.isOriginLocal() && !dataWriteCommand.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               svm.scheduleKeyInvalidation(dataWriteCommand.getKey(), ((NumericVersion) cacheEntry.getMetadata().version()).getVersion(), cacheEntry.isRemoved());
            }
            return null;
         });
      } else {
         return ctx.onReturn(dataWriteCommandNoReadHandler);
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
         cdl.notifyCommitEntry(created, removed, expired, entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         return false;
      }
   }

   private boolean commitSingleEntryIfNoChange(Object seenValue, NumericVersion seenVersion, CacheEntry entry, InvocationContext ctx, VisitableCommand command) {
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
         NumericVersion oldVersion = oldMetadata == null ? null : (NumericVersion) oldMetadata.version();
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
         cdl.notifyCommitEntry(created, removed, expired, entry, ctx, (FlagAffectedCommand) command, previousValue.get(), previousMetadata.get());
         return true;
      } else {
         return false;
      }
   }

   private NumericVersion getVersionOrNull(CacheEntry cacheEntry) {
      Metadata metadata = cacheEntry.getMetadata();
      if (metadata != null) {
         return (NumericVersion) metadata.version();
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

   private Address getNextMember(CacheTopology cacheTopology, Address address) {
      if (cacheTopology.getTopologyId() == cachedNextMemberTopology) {
         return cachedNextMember;
      }
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      List<Address> members = ch.getMembers();
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

   private CompletableFuture<Void> handleReadCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      // SKIP_OWNERSHIP_CHECK is added when the entry is prefetched from remote node
      checkTopology(command, cacheTopology);
      // TODO: local lookup and hinted read

      // ClusteredGetCommand invokes local-mode forced read, but we still have to check for primary owner
      // Scattered cache always uses only writeCH
      Address primaryOwner = getPrimary(cacheTopology.getTopologyId(), cacheTopology.getWriteConsistentHash(), command.getKey());
      if (cdl.getAddress().equals(primaryOwner)) {
         if (trace) {
            log.tracef("In topology %d this is primary owner", cacheTopology.getTopologyId());
         }
         return ctx.continueInvocation();
      } else if (command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK)) {
         if (trace) {
            log.trace("Ignoring ownership");
         }
         return ctx.continueInvocation();
      } else if (ctx.isOriginLocal()) {
         if (isLocalModeForced(command) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
            return ctx.continueInvocation();
         }
         ClusteredGetCommand clusteredGetCommand = cf.buildClusteredGetCommand(command.getKey(), command.getFlagsBitSet(), false, null);
         return retrieveRemoteValue(ctx, clusteredGetCommand, primaryOwner, cacheTopology.getTopologyId());
      } else {
         if (log.isDebugEnabled()) {
            log.debug("This node (" + rpcManager.getAddress() + ") is not an owner of segment "
               + cacheTopology.getWriteConsistentHash().getSegment(command.getKey()) + ", primary owner is " + primaryOwner);
         }
         return ctx.shortCircuit(UnsuccessfulResponse.UNSUCCESSFUL_EMPTY_RESPONSE);
      }
   }

   private Address getPrimary(int topologyId, ConsistentHash ch, Object key) {
      Address primaryOwner = ch.locatePrimaryOwner(key);
      if (primaryOwner == null) {
         throw new MissingOwnerException(topologyId);
      }
      return primaryOwner;
   }

   private CompletableFuture<Void> retrieveRemoteValue(InvocationContext ctx, ClusteredGetCommand clusteredGetCommand, Address primaryOwner, int topologyId) {
      CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(Collections.singletonList(primaryOwner), clusteredGetCommand, getRpcOptions);
      return rpcFuture.thenCompose(responseMap -> {
         Response response = getSingleResponse(responseMap, topologyId);
         if (response.isSuccessful()) {
            InternalCacheValue value = (InternalCacheValue) ((SuccessfulResponse) response).getResponseValue();
            if (value != null) {
               InternalCacheEntry cacheEntry = value.toInternalCacheEntry(clusteredGetCommand.getKey());
               entryFactory.wrapEntryForReading(ctx, clusteredGetCommand.getKey(), cacheEntry, false);
            }
            return ctx.continueInvocation();
         } else if (response.isValid()) {
            if (trace) {
               log.trace("Got unsuccessful response, will retry");
            }
            // TODO: it would be better to wait until the target can give us the right value
            CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
            Address newPrimaryOwner = getPrimary(cacheTopology.getTopologyId(), cacheTopology.getWriteConsistentHash(), clusteredGetCommand.getKey());
            return retrieveRemoteValue(ctx, clusteredGetCommand, newPrimaryOwner, cacheTopology.getTopologyId());
         } else {
            throw new CacheException("Invalid retrieval:" + response);
         }
      });
   }


   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      checkTopology(command, cacheTopology);

      Map<Object, Object> originalMap = command.getMap();
      if (command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
         extractAndSetMetadata(command, originalMap, ctx.getLookedUpEntries());
         return ctx.onReturn(putMapCommandHandler);
      }

      Address localAddress = rpcManager.getAddress();
      if (!ctx.isOriginLocal()) {
         Map<Object, CacheEntry> lookedUpEntries = ctx.getLookedUpEntries();
         if (command.isForwarded()) {
            // carries entries with version to back them up
            extractAndSetMetadata(command, originalMap, lookedUpEntries);
            return ctx.onReturn(putMapCommandHandler);
         } else {
            ConsistentHash ch = cacheTopology.getWriteConsistentHash();

            Map<Object, NumericVersion> versionMap = new HashMap<>(originalMap.size());
            for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
               Object key = entry.getKey();
               if (!cdl.localNodeIsPrimaryOwner(key)) {
                  throw new OutdatedTopologyException(localAddress + " no longer owns key " + key);
               }
               CacheEntry cacheEntry = lookedUpEntries.get(key);
               if (cacheEntry == null) {
                  throw new IllegalStateException("Not wrapped " + key);
               }
               NumericVersion version = new NumericVersion(svm.incrementVersion(ch.getSegment(key)));
               cacheEntry.setMetadata(addVersion(command.getMetadata(), version));
               versionMap.put(key, version);
            }
            return ctx.onReturn((ctx1, command1, rv, throwable) -> {
               for (Object key : ((PutMapCommand) command1).getAffectedKeys()) {
                  commitSingleEntryIfNewer(ctx1.lookupEntry(key), ctx1, command1);
               }
               return CompletableFuture.completedFuture(SuccessfulResponse.create(versionMap));
            });
         }
      }
      return ctx.onReturn((returnCtx, returnCommand, rv, throwable) -> {
         PutMapCommand putMapCommand = (PutMapCommand) returnCommand;
         if (!putMapCommand.isSuccessful()) {
            return null;
         }
         ConsistentHash ch = cacheTopology.getWriteConsistentHash();

         Map<Object, CacheEntry> lookedUpEntries = returnCtx.getLookedUpEntries();
         Map<Address, Map<Object, Object>> remoteEntries = new HashMap<>();
         Map<Object, InternalCacheValue> localEntries = new HashMap<>();
         for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
            Object key = entry.getKey();
            Address owner = getPrimary(cacheTopology.getTopologyId(), ch, key);
            if (localAddress.equals(owner)) {
               CacheEntry fromCtx = lookedUpEntries.get(key);
               if (fromCtx == null) {
                  throw new CacheException("Entry not looked up for " + key);
               }
               long version = svm.incrementVersion(ch.getSegment(key));
               Metadata metadata = new EmbeddedMetadata.Builder().version(new NumericVersion(version)).build();
               fromCtx.setMetadata(metadata);
               localEntries.put(key, new MetadataImmortalCacheValue(entry.getValue(), metadata));
               commitSingleEntryIfNewer(fromCtx, returnCtx, putMapCommand);
            } else {
               Map<Object, Object> currentEntries = remoteEntries.computeIfAbsent(owner, k -> new HashMap<>());
               currentEntries.put(key, entry.getValue());
            }
         }

         AtomicInteger expectedResponses = new AtomicInteger(remoteEntries.size());
         CompletableFuture<Object> allFuture = new CompletableFuture<>();
         if (!localEntries.isEmpty()) {
            Address backup = getNextMember(cacheTopology, localAddress);
            if (backup != null) {
               expectedResponses.incrementAndGet();
               // note: we abuse PutMapCommand a bit as we need it to transport versions as well, and it can
               // carry only single Metadata instance.
               PutMapCommand backupCommand = cf.buildPutMapCommand(localEntries, putMapCommand.getMetadata(), putMapCommand.getFlagsBitSet());
               backupCommand.setForwarded(true);
               backupCommand.addFlag(Flag.SKIP_LOCKING); // TODO: remove locking in handler
               rpcManager.invokeRemotelyAsync(Collections.singleton(backup), backupCommand, defaultSyncOptions).whenComplete((r, t) -> {
                  if (t != null) {
                     allFuture.completeExceptionally(t);
                  } else {
                     if (expectedResponses.decrementAndGet() == 0) {
                        allFuture.complete(rv);
                     }
                     for (Map.Entry<Object, InternalCacheValue> entry : localEntries.entrySet()) {
                        svm.scheduleKeyInvalidation(entry.getKey(), ((NumericVersion) entry.getValue().getMetadata().version()).getVersion(), false);
                     }
                  }
               });
            }
         }
         for (Map.Entry<Address, Map<Object, Object>> ownerEntry : remoteEntries.entrySet()) {
            Map<Object, Object> entries = ownerEntry.getValue();
            if (entries.isEmpty()) {
               if (expectedResponses.decrementAndGet() == 0) {
                  allFuture.complete(rv);
               }
               continue;
            }
            Address owner = ownerEntry.getKey();
            PutMapCommand toPrimary = cf.buildPutMapCommand(entries, putMapCommand.getMetadata(), putMapCommand.getFlagsBitSet());
            toPrimary.addFlag(Flag.SKIP_LOCKING); // TODO: remove locking in handler
            CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(
               Collections.singletonList(owner), toPrimary, defaultSyncOptions);
            rpcFuture.whenComplete((responseMap, t) -> {
               if (t != null) {
                  allFuture.completeExceptionally(t);
               }
               Response response = getSingleResponse(responseMap, putMapCommand.getTopologyId());
               if (response.isSuccessful()) {
                  Object responseValue = ((SuccessfulResponse) response).getResponseValue();
                  if (!(responseValue instanceof Map)) {
                     allFuture.completeExceptionally(new CacheException("Reponse from " + owner + ": expected Map<?, NumericVersion> but it is " + responseValue).fillInStackTrace());
                  }
                  Map<Object, NumericVersion> versions = (Map<Object, NumericVersion>) responseValue;
                  Map<Object, CacheEntry> retLookedUpEntries = returnCtx.getLookedUpEntries();
                  for (Map.Entry<Object, NumericVersion> entry : versions.entrySet()) {
                     CacheEntry fromCtx = retLookedUpEntries.get(entry.getKey());
                     if (fromCtx == null) {
                        allFuture.completeExceptionally(new CacheException(owner + " sent " + entry + " but " + entry.getKey() + " is not in the context").fillInStackTrace());
                     }
                     Metadata metadata = addVersion(putMapCommand.getMetadata(), entry.getValue());
                     fromCtx.setMetadata(metadata);
                     boolean committed = commitSingleEntryIfNewer(fromCtx, returnCtx, putMapCommand);;
                     if (committed && !putMapCommand.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
                        svm.scheduleKeyInvalidation(entry.getKey(), entry.getValue().getVersion(), false);
                     }
                  }
                  if (expectedResponses.decrementAndGet() == 0) {
                     allFuture.complete(rv);
                  }
               } else {
                  allFuture.completeExceptionally(new CacheException("Received unsuccessful response from " + owner + ": " + response));
               }
            });
         }
         return allFuture;
      });
   }

   protected static void extractAndSetMetadata(PutMapCommand command, Map<Object, Object> originalMap, Map<Object, CacheEntry> lookedUpEntries) {
      Map<Object, Object> valueMap = new HashMap<>(originalMap.size());
      for (Map.Entry<Object, Object> entry : originalMap.entrySet()) {
         Object key = entry.getKey();
         CacheEntry cacheEntry = lookedUpEntries.get(key);
         if (cacheEntry == null) {
            throw new IllegalStateException("Not wrapped " + key);
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
   public CompletableFuture<Void> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleReadCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      // The SKIP_OWNERSHIP_CHECK is added when the entries are prefetches from remote node
      checkTopology(command, cacheTopology);

      if (command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
         || command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK)) {
         return ctx.continueInvocation();
      }

      if (ctx.isOriginLocal()) {
         Address localAddress = cdl.getAddress();
         Map<Address, List<Object>> remoteKeys = new HashMap<>();
         for (Object key : command.getKeys()) {
            Address address = cdl.getPrimaryOwner(key);
            if (!localAddress.equals(address)) {
               remoteKeys.computeIfAbsent(address, k -> new ArrayList<>()).add(key);
            }
         }

         if (remoteKeys.isEmpty()) {
            return ctx.continueInvocation();
         }
         ResponseSync sync = new ResponseSync(remoteKeys.size(), cacheTopology.getTopologyId());
         for (Map.Entry<Address, List<Object>> remote : remoteKeys.entrySet()) {
            ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(remote.getValue(), command.getFlagsBitSet(), null);
            CompletableFuture<Map<Address, Response>> rpcFuture = rpcManager.invokeRemotelyAsync(Collections.singleton(remote.getKey()), clusteredGetAllCommand, defaultSyncOptions);
            rpcFuture.whenComplete(((responseMap, throwable) -> handleGetAllResponse(responseMap, throwable, ctx, clusteredGetAllCommand, sync)));
         }
         return sync.thenRun(() -> ctx.continueInvocation());
      } else { // remote
         for (Object key : command.getKeys()) {
            if (!cdl.localNodeIsPrimaryOwner(key)) {
               if (log.isDebugEnabled()) {
                  int segment = cacheTopology.getWriteConsistentHash().getSegment(key);
                  log.debug("This node (" + rpcManager.getAddress() + ") is not an owner of segment "
                     + segment + ", primary owner is " + cacheTopology.getWriteConsistentHash().locatePrimaryOwnerForSegment(segment));
               }
               // TODO: We cannot use UnsuccessfulResponse as EntryWrappingInterceptor casts result to map for notification
               return ctx.shortCircuit(null);
            }
         }
         return ctx.continueInvocation();
      }
   }

   private void handleGetAllResponse(Map<Address, Response> responseMap, Throwable throwable, InvocationContext ctx,
                                     ClusteredGetAllCommand command, ResponseSync sync) {
      if (throwable != null) {
         sync.completeExceptionally(throwable);
      } else {
         try {
            Response response = getSingleResponse(responseMap, sync.getTopologyId());
            Address sender = responseMap.keySet().iterator().next();
            if (response.isSuccessful()) {
               List<InternalCacheValue> values = (List<InternalCacheValue>) ((SuccessfulResponse) response).getResponseValue();
               if (values != null) {
                  Iterator<Object> keyIterator = command.getKeys().iterator();
                  Iterator<InternalCacheValue> valueIterator = values.iterator();
                  synchronized (sync) {
                     while (keyIterator.hasNext() && valueIterator.hasNext()) {
                        Object key = keyIterator.next();
                        InternalCacheValue next = valueIterator.next();
                        if (next != null) {
                           entryFactory.wrapEntryForReading(ctx, key, next.toInternalCacheEntry(key), false);
                        }
                     }
                     if (keyIterator.hasNext() || valueIterator.hasNext()) {
                        sync.completeExceptionally(new CacheException("Number of keys/values does not match").fillInStackTrace());
                     }
                     if (--sync.expectedResponses == 0) {
                        sync.complete(null);
                     }
                  }
               } else {
                  // the result is null because the node is not primary owner of those entries
                  CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
                  log.tracef("Response is null, current topology %d, request topology %d", cacheTopology.getTopologyId(), sync.getTopologyId());
                  if (cacheTopology.getTopologyId() == sync.getTopologyId()) {
                     rpcManager.invokeRemotelyAsync(Collections.singleton(sender), command, defaultSyncOptions)
                        .whenComplete((responseMap1, throwable1) -> handleGetAllResponse(responseMap1, throwable1, ctx, command, sync));
                  } else {
                     sync.completeExceptionally(new OutdatedTopologyException("Request topology " + sync.getTopologyId() + " cache topology " + cacheTopology.getTopologyId()));
                  }
               }
            } else {
               sync.completeExceptionally(new CacheException("Unsuccessful response: " + response));
            }
         } catch (RuntimeException e) {
            sync.completeExceptionally(e);
         }
      }
   }

   @Override
   public CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // local mode clear will have unpredictable results
      svm.clearInvalidations();
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(
            isSynchronous(command) ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS)
            .build();
         rpcManager.invokeRemotely(null, command, rpcOptions);
      }
      return ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null) {
            throw throwable;
         }
         cdl.commitEntry(ClearCacheEntry.getInstance(), null, (ClearCommand) rCommand, rCtx, null, false);
         return null;
      });
   }

   protected static Response getSingleResponse(Map<Address, Response> responseMap, int topologyId) {
      if (responseMap.isEmpty()) {
         if (trace) log.trace("No response!");
         return null;
      } else if (responseMap.size() > 1) {
         throw new IllegalArgumentException("Expected single response, got " + responseMap);
      }
      Response response = responseMap.values().iterator().next();
      if (response.isValid()) {
         return response;
      }
      if (response instanceof CacheNotFoundResponse) {
         // This means the cache wasn't running on the primary owner, so the command wasn't executed.
         // It is also possible that the primary owner is not a member of view, as we're degraded.
         throw new MissingOwnerException(topologyId);
      }
      Throwable cause = null;
      if (response instanceof ExceptionResponse) {
         cause = ((ExceptionResponse) response).getException();
      }
      throw new CacheException("Got unsuccessful response from primary owner: " + response, cause);
   }

   @Override
   public CompletableFuture<Void> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Void> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CompletableFuture<Void> visitGetKeysInGroupCommand(InvocationContext ctx,
                                                                   GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return ctx.continueInvocation();
      }
      CompletableFuture<Map<Address, Response>> future = rpcManager
         .invokeRemotelyAsync(Collections.singleton(groupManager.getPrimaryOwner(groupName)), command,
            rpcManager.getDefaultRpcOptions(true));
      return future.thenCompose(responses -> {
         if (!responses.isEmpty()) {
            Response response = responses.values().iterator().next();
            if (response instanceof SuccessfulResponse) {
               //noinspection unchecked
               List<CacheEntry> cacheEntries =
                  (List<CacheEntry>) ((SuccessfulResponse) response).getResponseValue();
               for (CacheEntry entry : cacheEntries) {
                  entryFactory.wrapExternalEntry(ctx, entry.getKey(), entry, EntryFactory.Wrap.STORE, false);
               }
            }
         }
         return ctx.continueInvocation();
      });
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private static class ResponseSync extends CompletableFuture<Void> {
      private int expectedResponses;
      private final int topologyId;

      public ResponseSync(int expectedResponses, int topologyId) {
         this.expectedResponses = expectedResponses;
         this.topologyId = topologyId;
      }

      public int getTopologyId() {
         return topologyId;
      }
   }
}
