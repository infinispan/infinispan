package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.infinispan.commons.util.Util.toStr;

/**
 * Base class for distribution of entries across a cluster.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dan@infinispan.org>
 */
public abstract class BaseDistributionInterceptor extends ClusteringInterceptor {

   protected DistributionManager dm;

   protected ClusteringDependentLogic cdl;
   protected RemoteValueRetrievedListener rvrl;
   protected boolean isL1Enabled;
   private GroupManager groupManager;

   private static final Log log = LogFactory.getLog(BaseDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager, ClusteringDependentLogic cdl,
         RemoteValueRetrievedListener rvrl, GroupManager groupManager) {
      this.dm = distributionManager;
      this.cdl = cdl;
      this.rvrl = rvrl;
      this.groupManager = groupManager;
   }


   @Start
   public void configure() {
      // Can't rely on the super injectConfiguration() to be called before our injectDependencies() method2
      isL1Enabled = cacheConfiguration.clustering().l1().enabled();
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
   public final CompletableFuture<Void> visitClearCommand(InvocationContext ctx, ClearCommand command)
         throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(
               isSynchronous(command) ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS)
               .build();
         return rpcManager
               .invokeRemotelyAsync(null, command, rpcOptions)
               .thenCompose(responses -> ctx.continueInvocation());
      }
      return ctx.continueInvocation();
   }

   protected final CompletableFuture<InternalCacheEntry> retrieveFromProperSource(Object key,
         InvocationContext ctx, boolean acquireRemoteLock, FlagAffectedCommand command, boolean isWrite)
         throws Exception {
      return doRetrieveFromProperSource(key, null, -1, ctx, command, acquireRemoteLock, isWrite);
   }

   private CompletableFuture<InternalCacheEntry> doRetrieveFromProperSource(Object key, InternalCacheEntry value,
         int lastTopologyId, InvocationContext ctx, FlagAffectedCommand command, boolean acquireRemoteLock,
         boolean isWrite) {
      if (value != null)
         return CompletableFuture.completedFuture(value);

      final CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      final int currentTopologyId = cacheTopology.getTopologyId();

      if (trace) {
            log.tracef("Perform remote get for key %s. topologyId=%s, currentTopologyId=%s",
                       key, lastTopologyId, currentTopologyId);
      }
      List<Address> targets;
      int newTopologyId;
      if (lastTopologyId < currentTopologyId) {
         // Cache topology has changed or it is the first time.
         newTopologyId = currentTopologyId;
         ConsistentHash readCH = cacheTopology.getReadConsistentHash();
         boolean isLocal;
         List<Address> owners;
         if (readCH.isReplicated()) {
            isLocal = readCH.isSegmentLocalToNode(rpcManager.getAddress(), 0);
            owners = readCH.getMembers();
         } else {
            owners = readCH.locateOwners(key);
            isLocal = owners.contains(rpcManager.getAddress());
         }
         if (isLocal) {
            // Normally looking the value up in the local data container doesn't help, because we already
            // tried to read it in the EntryWrappingInterceptor.
            // But ifIf we became an owner in the read CH after EntryWrappingInterceptor, we may not find the
            // value on the remote nodes (e.g. because the local node is now the only owner).
            return CompletableFuture.completedFuture(dataContainer.get(key));
         } else {
            targets = new ArrayList<>(owners);
         }
      } else if (lastTopologyId == currentTopologyId && cacheTopology.getPendingCH() != null) {
         // Same topologyId, but the owners could have already installed the next topology
         // Lets try with pending consistent owners (the read owners in the next topology)
         newTopologyId = currentTopologyId + 1;
         targets = new ArrayList<>(cacheTopology.getPendingCH().locateOwners(key));
         // Remove already contacted nodes
         targets.removeAll(cacheTopology.getReadConsistentHash().locateOwners(key));
         if (targets.isEmpty()) {
            if (trace) {
               log.tracef("No valid values found for key '%s' (topologyId=%s).", key, currentTopologyId);
            }
            return CompletableFutures.completedNull();
         }
         } else { // lastTopologyId > currentTopologyId || cacheTopology.getPendingCH() == null
            // We have not received a valid value from the pending CH owners either, and the topology id hasn't changed
         if (trace) {
            log.tracef("No valid values found for key '%s' (topologyId=%s).", key, currentTopologyId);
         }
         return CompletableFutures.completedNull();
      }
      
      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      ClusteredGetCommand getCommand =
            cf.buildClusteredGetCommand(key, command.getFlagsBitSet(), acquireRemoteLock, gtx);
      getCommand.setWrite(isWrite);

      RpcOptionsBuilder rpcOptionsBuilder =
            rpcManager.getRpcOptionsBuilder(ResponseMode.WAIT_FOR_VALID_RESPONSE, DeliverOrder.NONE);
      return invokeClusterGetCommandRemotely(targets, rpcOptionsBuilder, getCommand, key).thenCompose(
            newValue -> doRetrieveFromProperSource(key, newValue, newTopologyId, ctx, command, acquireRemoteLock,
                  isWrite));
   }

   private CompletableFuture<InternalCacheEntry> invokeClusterGetCommandRemotely(List<Address> targets,
         RpcOptionsBuilder rpcOptionsBuilder, ClusteredGetCommand get, Object key) {
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, rpcManager.getAddress());
      RpcOptions options = rpcOptionsBuilder.responseFilter(filter).build();
      return rpcManager.invokeRemotelyAsync(targets, get, options).thenApply(responses -> {
         if (!responses.isEmpty()) {
            for (Response r : responses.values()) {
               if (r instanceof SuccessfulResponse) {
                  // The response value might be null.
                  SuccessfulResponse response = (SuccessfulResponse) r;
                  Object responseValue = response.getResponseValue();
                  if (responseValue == null) {
                     continue;
                  }

                  InternalCacheValue cacheValue = (InternalCacheValue) responseValue;
                  InternalCacheEntry ice = cacheValue.toInternalCacheEntry(key);
                  if (rvrl != null) {
                     rvrl.remoteValueFound(ice);
                  }
                  return ice;
               }
            }
         }
         if (rvrl != null) {
            rvrl.remoteValueNotFound(key);
         }
         return null;
      });
   }

   protected Map<Object, InternalCacheEntry> retrieveFromRemoteSources(Set<?> requestedKeys, InvocationContext ctx, long flagsBitSet) throws Throwable {
      GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      ConsistentHash ch = cacheTopology.getReadConsistentHash();

      Map<Address, List<Object>> ownerKeys = new HashMap<>();
      for (Object key : requestedKeys) {
         Address owner = ch.locatePrimaryOwner(key);
         List<Object> requestedKeysFromNode = ownerKeys.get(owner);
         if (requestedKeysFromNode == null) {
            ownerKeys.put(owner, requestedKeysFromNode = new ArrayList<>());
         }
         requestedKeysFromNode.add(key);
      }

      Map<Address, ReplicableCommand> commands = new HashMap<>();
      for (Map.Entry<Address, List<Object>> entry : ownerKeys.entrySet()) {
         List<Object> keys = entry.getValue();
         ClusteredGetAllCommand remoteGetAll = cf.buildClusteredGetAllCommand(keys, flagsBitSet, gtx);
         commands.put(entry.getKey(), remoteGetAll);
      }

      RpcOptionsBuilder rpcOptionsBuilder = rpcManager.getRpcOptionsBuilder(
            ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE);
      RpcOptions options = rpcOptionsBuilder.build();
      Map<Address, Response> responses = rpcManager.invokeRemotely(commands, options);

      Map<Object, InternalCacheEntry> entries = new HashMap<>();
      for (Map.Entry<Address, Response> entry : responses.entrySet()) {
         updateWithValues(((ClusteredGetAllCommand) commands.get(entry.getKey())).getKeys(),
               entry.getValue(), entries);
      }

      return entries;
   }

   private void updateWithValues(List<?> keys, Response r, Map<Object, InternalCacheEntry> entries) {
      if (r instanceof SuccessfulResponse) {
         SuccessfulResponse response = (SuccessfulResponse) r;
         List<InternalCacheValue> values = (List<InternalCacheValue>) response.getResponseValue();
         // Only process if we got a return value - this can happen if the node is shutting
         // down when it received the request
         if (values != null) {
            for (int i = 0; i < keys.size(); ++i) {
               InternalCacheValue icv = values.get(i);
               if (icv != null) {
                  Object key = keys.get(i);
                  Object value = icv.getValue();
                  if (value == null) {
                     entries.put(key, null);
                  } else {
                     InternalCacheEntry ice = icv.toInternalCacheEntry(key);
                     entries.put(key, ice);
                  }
               }
            }
         }
      }
   }

   protected final CompletableFuture<Void> handleNonTxWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      if (ctx.isInTxScope()) {
         throw new CacheException("Attempted execution of non-transactional write command in a transactional invocation context");
      }

      // invoke the command locally, we need to know if it's successful or not
      ctx.onReturn((rCtx, rCommand, rv, throwable) -> {
         if (throwable != null)
            throw throwable;

         return handleLocalResult(ctx, command, rv);
      });

      // see if we need to load values from remote sources first
      return remoteGetBeforeWrite(ctx, command, command.getKey());
   }

   private CompletableFuture<Object> handleLocalResult(InvocationContext ctx, DataWriteCommand command,
         Object localResult) {
      // if this is local mode then skip distributing
      if (isLocalModeForced(command)) {
         return CompletableFuture.completedFuture(localResult);
      }

      return invokeRemotelyIfNeeded(ctx, command, localResult);
   }

   private CompletableFuture<Object> invokeRemotelyIfNeeded(InvocationContext ctx, DataWriteCommand command,
         Object localResult) {
      boolean isSync = isSynchronous(command);
      Address primaryOwner = cdl.getPrimaryOwner(command.getKey());
      int commandTopologyId = command.getTopologyId();
      int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
      // TotalOrderStateTransferInterceptor doesn't set the topology id for PFERs.
      // TODO Shouldn't PFERs be executed in a tx with total order?
      boolean topologyChanged = isSync && currentTopologyId != commandTopologyId && commandTopologyId != -1;
      if (trace) {
         log.tracef("Command topology id is %d, current topology id is %d, successful? %s",
               (Object) commandTopologyId, currentTopologyId, command.isSuccessful());
      }
      // We need to check for topology changes on the origin even if the command was unsuccessful
      // otherwise we execute the command on the correct primary owner, and then we still
      // throw an OutdatedTopologyInterceptor when we return in EntryWrappingInterceptor.
      if (topologyChanged) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                     commandTopologyId + ", got " + currentTopologyId);
      }

      ValueMatcher valueMatcher = command.getValueMatcher();
      if (!ctx.isOriginLocal()) {
         if (primaryOwner.equals(rpcManager.getAddress())) {
            if (!command.isSuccessful()) {
               if (trace) log.tracef("Skipping the replication of the conditional command as it did not succeed on primary owner (%s).", command);
               return CompletableFuture.completedFuture(localResult);
            }
            List<Address> recipients = cdl.getOwners(command.getKey());
            // Ignore the previous value on the backup owners
            command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
            try {
               rpcManager.invokeRemotely(recipients, command, determineRpcOptionsForBackupReplication(rpcManager,
                                                                                                      isSync, recipients));
            } finally {
               // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
               command.setValueMatcher(valueMatcher.matcherForRetry());
            }
         }
         return CompletableFuture.completedFuture(localResult);
      } else {
         if (primaryOwner.equals(rpcManager.getAddress())) {
            if (!command.isSuccessful()) {
               if (trace) log.tracef("Skipping the replication of the command as it did not succeed on primary owner (%s).", command);
               return CompletableFuture.completedFuture(localResult);
            }
            List<Address> recipients = cdl.getOwners(command.getKey());
            if (trace) log.tracef("I'm the primary owner, sending the command to all the backups (%s) in order to be applied.",
                  recipients);
            // check if a single owner has been configured and the target for the key is the local address
            boolean isSingleOwnerAndLocal = cacheConfiguration.clustering().hash().numOwners() == 1;
            if (!isSingleOwnerAndLocal) {
               // Ignore the previous value on the backup owners
               command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
               try {
                  rpcManager.invokeRemotely(recipients, command, determineRpcOptionsForBackupReplication(rpcManager,
                                                                                                         isSync, recipients));
               } finally {
                  // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
                  command.setValueMatcher(valueMatcher.matcherForRetry());
               }
            }
            return CompletableFuture.completedFuture(localResult);
         } else {
            if (trace) log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
            boolean isSyncForwarding = isSync || command.isReturnValueExpected();

            Map<Address, Response> addressResponseMap;
            try {
               addressResponseMap = rpcManager.invokeRemotely(Collections.singletonList(primaryOwner), command,
                           rpcManager.getDefaultRpcOptions(isSyncForwarding));
            } finally {
               command.setValueMatcher(valueMatcher.matcherForRetry());
            }
            if (!isSyncForwarding)
               return CompletableFuture.completedFuture(localResult);

            Object primaryResult = getResponseFromPrimaryOwner(primaryOwner, addressResponseMap);
            command.updateStatusFromRemoteResponse(primaryResult);
            return CompletableFuture.completedFuture(primaryResult);
         }
      }
   }

   private RpcOptions determineRpcOptionsForBackupReplication(RpcManager rpc, boolean isSync, List<Address> recipients) {
      RpcOptions options;
      if (isSync) {
         // If no recipients, means a broadcast, so we can ignore leavers
         if (recipients == null) {
            options = rpc.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS).build();
         } else {
            options = rpc.getDefaultRpcOptions(true);
         }
      } else {
         options = rpc.getDefaultRpcOptions(false);
      }
      return options;
   }

   private Object getResponseFromPrimaryOwner(Address primaryOwner, Map<Address, Response> addressResponseMap) {
      Response fromPrimaryOwner = addressResponseMap.get(primaryOwner);
      if (fromPrimaryOwner == null) {
         if (trace) log.tracef("Primary owner %s returned null", primaryOwner);
         return null;
      }
      if (fromPrimaryOwner.isSuccessful()) {
         return ((SuccessfulResponse) fromPrimaryOwner).getResponseValue();
      }

      if (addressResponseMap.get(primaryOwner) instanceof CacheNotFoundResponse) {
         // This means the cache wasn't running on the primary owner, so the command wasn't executed.
         // We throw an OutdatedTopologyException, StateTransferInterceptor will catch the exception and
         // it will then retry the command.
         throw new OutdatedTopologyException("Cache is no longer running on primary owner " + primaryOwner);
      }

      Throwable cause = fromPrimaryOwner instanceof ExceptionResponse ? ((ExceptionResponse)fromPrimaryOwner).getException() : null;
      throw new CacheException("Got unsuccessful response from primary owner: " + fromPrimaryOwner, cause);
   }

   @Override
   public CompletableFuture<Void> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)
            || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         return ctx.continueInvocation();
      }

      int commandTopologyId = command.getTopologyId();
      if (ctx.isOriginLocal()) {
         int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
         boolean topologyChanged = currentTopologyId != commandTopologyId && commandTopologyId != -1;
         if (trace) {
            log.tracef("Command topology id is %d, current topology id is %d", commandTopologyId, currentTopologyId);
         }
         if (topologyChanged) {
            throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                        commandTopologyId + ", got " + currentTopologyId);
         }

         // At this point, we know that an entry located on this node that exists in the data container/store
         // must also exist in the context.
         ConsistentHash ch = command.getConsistentHash();
         Set<Object> requestedKeys = new HashSet<>();
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null) {
               if (!ch.isKeyLocalToNode(rpcManager.getAddress(), key)) {
                  requestedKeys.add(key);
               } else {
                  if (trace) {
                     log.tracef("Not doing a remote get for missing key %s since entry is "
                                 + "mapped to current node (%s). Owners are %s",
                           toStr(key), rpcManager.getAddress(), ch.locateOwners(key));
                  }
                  // Force a map entry to be created, because we know this entry is local
                  entryFactory.wrapExternalEntry(ctx, key, null, EntryFactory.Wrap.WRAP_ALL, false);
               }
            }
         }

         boolean missingRemoteValues = false;
         if (!requestedKeys.isEmpty()) {
            if (trace) {
               log.tracef("Fetching entries for keys %s from remote nodes", requestedKeys);
            }

            Map<Object, InternalCacheEntry> justRetrieved = retrieveFromRemoteSources(
                  requestedKeys, ctx, command.getFlagsBitSet());
            Map<Object, InternalCacheEntry> previouslyFetched = command.getRemotelyFetched();
            if (previouslyFetched != null) {
               previouslyFetched.putAll(justRetrieved);
            } else {
               command.setRemotelyFetched(justRetrieved);
            }
            for (Object key : requestedKeys) {
               if (!justRetrieved.containsKey(key)) {
                  missingRemoteValues = true;
               } else {
                  InternalCacheEntry remoteEntry = justRetrieved.get(key);
                  entryFactory.wrapExternalEntry(ctx, key, remoteEntry, EntryFactory.Wrap.WRAP_NON_NULL,
                                                 false);
               }
            }
         }

         if (missingRemoteValues) {
            throw new OutdatedTopologyException("Remote values are missing because of a topology change");
         }
         return ctx.continueInvocation();
      } else { // remote
         int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
         boolean topologyChanged = currentTopologyId != commandTopologyId && commandTopologyId != -1;
         // If the topology changed while invoking, this means we cannot trust any null values
         // so we shouldn't return them
         ConsistentHash ch = command.getConsistentHash();
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null || entry.isNull()) {
               if (ch.isKeyLocalToNode(rpcManager.getAddress(), key) &&
                     !topologyChanged) {
                  if (trace) {
                     log.tracef("Not doing a remote get for missing key %s since entry is "
                                 + "mapped to current node (%s). Owners are %s",
                           toStr(key), rpcManager.getAddress(), ch.locateOwners(key));
                  }
                  // Force a map entry to be created, because we know this entry is local
                  entryFactory.wrapExternalEntry(ctx, key, null, EntryFactory.Wrap.WRAP_ALL, false);
               }
            }
         }
         return ctx.continueInvocation();
      }
   }

   @Override
   public CompletableFuture<Void> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      // TODO: Can we reimplement GetAll in terms of ReadOnlyManyCommand?
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL)
         || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         return ctx.continueInvocation();
      }

      int commandTopologyId = command.getTopologyId();
      if (ctx.isOriginLocal()) {
         int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
         boolean topologyChanged = currentTopologyId != commandTopologyId && commandTopologyId != -1;
         if (trace) {
            log.tracef("Command topology id is %d, current topology id is %d", commandTopologyId, currentTopologyId);
         }
         if (topologyChanged) {
            throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
                        commandTopologyId + ", got " + currentTopologyId);
         }

         // At this point, we know that an entry located on this node that exists in the data container/store
         // must also exist in the context.
         ConsistentHash ch = command.getConsistentHash();
         Set<Object> requestedKeys = new HashSet<>();
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null) {
               if (!ch.isKeyLocalToNode(rpcManager.getAddress(), key)) {
                  requestedKeys.add(key);
               } else {
                  if (trace) {
                     log.tracef("Not doing a remote get for missing key %s since entry is "
                           + "mapped to current node (%s). Owners are %s",
                        toStr(key), rpcManager.getAddress(), ch.locateOwners(key));
                  }
                  // Force a map entry to be created, because we know this entry is local
                  entryFactory.wrapExternalEntry(ctx, key, null, EntryFactory.Wrap.WRAP_ALL, false);
               }
            }
         }

         boolean missingRemoteValues = false;
         if (!requestedKeys.isEmpty()) {
            if (trace) {
               log.tracef("Fetching entries for keys %s from remote nodes", requestedKeys);
            }

            Map<Object, InternalCacheEntry> justRetrieved = retrieveFromRemoteSources(
               requestedKeys, ctx, command.getFlagsBitSet());
            Map<Object, InternalCacheEntry> previouslyFetched = command.getRemotelyFetched();
            if (previouslyFetched != null) {
               previouslyFetched.putAll(justRetrieved);
            } else {
               command.setRemotelyFetched(justRetrieved);
            }
            for (Object key : requestedKeys) {
               if (!justRetrieved.containsKey(key)) {
                  missingRemoteValues = true;
               } else {
                  InternalCacheEntry remoteEntry = justRetrieved.get(key);
                  entryFactory.wrapExternalEntry(ctx, key, remoteEntry, EntryFactory.Wrap.WRAP_NON_NULL,
                                                 false);
               }
            }
         }

         if (missingRemoteValues) {
            throw new OutdatedTopologyException("Remote values are missing because of a topology change");
         }
         return ctx.continueInvocation();
      } else { // remote
         int currentTopologyId = stateTransferManager.getCacheTopology().getTopologyId();
         boolean topologyChanged = currentTopologyId != commandTopologyId && commandTopologyId != -1;
         // If the topology changed while invoking, this means we cannot trust any null values
         // so we shouldn't return them
         ConsistentHash ch = command.getConsistentHash();
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null || entry.isNull()) {
               if (ch.isKeyLocalToNode(rpcManager.getAddress(), key) &&
                     !topologyChanged) {
                  if (trace) {
                     log.tracef("Not doing a remote get for missing key %s since entry is "
                           + "mapped to current node (%s). Owners are %s",
                        toStr(key), rpcManager.getAddress(), ch.locateOwners(key));
                  }
                  // Force a map entry to be created, because we know this entry is local
                  entryFactory.wrapExternalEntry(ctx, key, null, EntryFactory.Wrap.WRAP_ALL, false);
               }
            }
         }
         return ctx.continueInvocation();
      }
   }

   /**
    * @return Whether a remote get is needed to obtain the previous values of the affected entries.
    */
   protected abstract boolean writeNeedsRemoteValue(InvocationContext ctx, WriteCommand command, Object key);

   protected boolean valueIsMissing(CacheEntry entry) {
      return entry == null || (entry.isNull() && !entry.isRemoved() && !entry.skipLookup());
   }

   protected abstract CompletableFuture<Void> remoteGetBeforeWrite(InvocationContext ctx,
         WriteCommand command, Object key) throws Throwable;

   /**
    * @return {@code true} if the value is not available on the local node and a read command is allowed to
    * fetch it from a remote node. Does not check if the value is already in the context.
    */
   protected boolean readNeedsRemoteValue(InvocationContext ctx, AbstractDataCommand command) {
      return ctx.isOriginLocal() && !command.hasFlag(Flag.CACHE_MODE_LOCAL) &&
            !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP);
   }
}
