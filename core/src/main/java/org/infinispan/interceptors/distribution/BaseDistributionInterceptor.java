package org.infinispan.interceptors.distribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.AbstractDataWriteCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.Ownership;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   public final BasicInvocationStage visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable {
      final String groupName = command.getGroupName();
      if (command.isGroupOwner()) {
         //don't go remote if we are an owner.
         return invokeNext(ctx, command);
      }
      CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(
            Collections.singleton(groupManager.getPrimaryOwner(groupName)), command,
            rpcManager.getDefaultRpcOptions(true));
      return invokeNextAsync(ctx, command, future.thenAccept(responses -> {
         if (!responses.isEmpty()) {
            Response response = responses.values().iterator().next();
            if (response instanceof SuccessfulResponse) {
               //noinspection unchecked
               List<CacheEntry> cacheEntries = (List<CacheEntry>) ((SuccessfulResponse) response).getResponseValue();
               for (CacheEntry entry : cacheEntries) {
                  entryFactory.wrapExternalEntry(ctx, entry.getKey(), entry, false, false);
               }
            }
         }
      }));
   }

   @Override
   public final BasicInvocationStage visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isLocalModeForced(command)) {
         RpcOptions rpcOptions = rpcManager.getRpcOptionsBuilder(
               isSynchronous(command) ? ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS : ResponseMode.ASYNCHRONOUS).build();
         return invokeNextAsync(ctx, command, rpcManager.invokeRemotelyAsync(null, command, rpcOptions));
      }
      return invokeNext(ctx, command);
   }

   protected final CompletableFuture<Void> remoteGet(InvocationContext ctx, AbstractDataCommand command,
                                                     boolean isWrite) throws Exception {
      final CacheTopology cacheTopology = checkTopologyId(command);

      Object key = command.getKey();
      ConsistentHash readCH = cacheTopology.getReadConsistentHash();
      DistributionInfo info = new DistributionInfo(key, readCH, rpcManager.getAddress());
      if (info.ownership() != Ownership.NON_OWNER) {
         if (trace) {
            log.tracef("Key %s is local, skipping remote get", key);
         }
         return CompletableFutures.completedNull();
      }

      if (trace) {
         log.tracef("Perform remote get for key %s. currentTopologyId=%s, owners=%s",
            key, cacheTopology.getTopologyId(), info.owners());
      }

      ClusteredGetCommand getCommand = cf.buildClusteredGetCommand(key, command.getFlagsBitSet());
      getCommand.setTopologyId(cacheTopology.getTopologyId());
      getCommand.setWrite(isWrite);

      return rpcManager.invokeRemotelyAsync(info.owners(), getCommand, staggeredOptions).thenApply(responses -> {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               SuccessfulResponse response = (SuccessfulResponse) r;
               Object responseValue = response.getResponseValue();
               if (responseValue == null) {
                  if (rvrl != null) {
                     rvrl.remoteValueNotFound(key);
                  }
                  return null;
               }
               InternalCacheEntry ice = ((InternalCacheValue) responseValue).toInternalCacheEntry(key);
               if (rvrl != null) {
                  rvrl.remoteValueFound(ice);
               }
               return ice;
            }
         }
         // We cannot throw OutdatedTopologyException because this can happen when we have already actual topology
         // but the other nodes have not, yet.
         // TODO: we need to retry until we get a timeout, but the time should sum up for all responses
         throw new RpcException("Did not get any successful response, got " + responses);
      }).thenAccept(ice -> entryFactory.wrapExternalEntry(ctx, key, ice, isWrite, false));
   }

   protected final BasicInvocationStage handleNonTxWriteCommand(InvocationContext ctx, AbstractDataWriteCommand command)
         throws Throwable {
      if (ctx.isInTxScope()) {
         throw new CacheException("Attempted execution of non-transactional write command in a transactional invocation context");
      }

      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry == null) {
         if (isLocalModeForced(command)) {
            entryFactory.wrapExternalEntry(ctx, key, null, true, true);
            return invokeNext(ctx, command);
         } else {
            DistributionInfo info = new DistributionInfo(key, checkTopologyId(command).getWriteConsistentHash(), rpcManager.getAddress());
            boolean load = shouldLoad(ctx, command, info);
            if (info.isPrimary()) {
               if (load) {
                  CompletableFuture<?> getFuture = remoteGet(ctx, command, true);
                  return invokeNextAsync(ctx, command, getFuture).thenCompose(this::primaryReturnHandler);
               } else {
                  entryFactory.wrapExternalEntry(ctx, command, null, true, true);
                  return invokeNext(ctx, command).thenCompose(this::primaryReturnHandler);
               }
            } else if (ctx.isOriginLocal()) {
               return invokeRemotely(command, info.primary());
            } else {
               if (load) {
                  CompletableFuture<?> getFuture = remoteGet(ctx, command, true);
                  return invokeNextAsync(ctx, command, getFuture);
               } else {
                  entryFactory.wrapExternalEntry(ctx, key, null, true, true);
                  return invokeNext(ctx, command);
               }
            }
         }
      } else if (command.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         return invokeNext(ctx, command);
      } else {
         DistributionInfo info = new DistributionInfo(key, checkTopologyId(command).getWriteConsistentHash(), rpcManager.getAddress());
         if (info.isPrimary()) {
            return invokeNext(ctx, command).thenCompose(this::primaryReturnHandler);
         } else if (ctx.isOriginLocal()) {
            return invokeRemotely(command, info.primary());
         } else {
            return invokeNext(ctx, command);
         }
      }
   }

   private boolean shouldLoad(InvocationContext ctx, AbstractDataWriteCommand command, DistributionInfo info) {
      if (!command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         VisitableCommand.LoadType loadType = command.loadType();
         switch (loadType) {
            case DONT_LOAD:
               return false;
            case OWNER:
               switch (info.ownership()) {
                  case PRIMARY:
                     return true;
                  case BACKUP:
                     return !ctx.isOriginLocal();
                  case NON_OWNER:
                     return false;
                  default:
                     throw new IllegalStateException();
               }
            case PRIMARY:
               return info.isPrimary();
            default:
               throw new IllegalStateException();
         }
      } else {
         return false;
      }
   }

   private BasicInvocationStage invokeRemotely(DataWriteCommand command, Address primaryOwner) {
      if (trace) log.tracef("I'm not the primary owner, so sending the command to the primary owner(%s) in order to be forwarded", primaryOwner);
      boolean isSyncForwarding = isSynchronous(command) || command.isReturnValueExpected();

      CompletableFuture<Map<Address, Response>> remoteInvocation;
      try {
         remoteInvocation = rpcManager.invokeRemotelyAsync(Collections.singletonList(primaryOwner), command,
               rpcManager.getDefaultRpcOptions(isSyncForwarding));
      } catch (Throwable t) {
         command.setValueMatcher(command.getValueMatcher().matcherForRetry());
         throw t;
      }
      if (isSyncForwarding) {
         return returnWithAsync(remoteInvocation.handle((responses, t) -> {
            command.setValueMatcher(command.getValueMatcher().matcherForRetry());
            CompletableFutures.rethrowException(t);

            Object primaryResult = getResponseFromPrimaryOwner(primaryOwner, responses);
            command.updateStatusFromRemoteResponse(primaryResult);
            return primaryResult;
         }));
      } else {
         return returnWith(null);
      }
   }

   private BasicInvocationStage primaryReturnHandler(BasicInvocationStage ignored, InvocationContext ctx, VisitableCommand visitableCommand, Object localResult) {
      DataWriteCommand command = (DataWriteCommand) visitableCommand;
      if (!command.isSuccessful()) {
         if (trace) log.tracef("Skipping the replication of the conditional command as it did not succeed on primary owner (%s).", command);
         return returnWith(localResult);
      }
      // check if a single owner has been configured and the target for the key is the local address
      boolean isSingleOwnerAndLocal = cacheConfiguration.clustering().hash().numOwners() == 1;
      if (isSingleOwnerAndLocal) {
         return returnWith(localResult);
      }
      ConsistentHash writeCH = checkTopologyId(command).getWriteConsistentHash();
      List<Address> recipients = writeCH.isReplicated() ? null : writeCH.locateOwners(command.getKey());

      // Cache the matcher and reset it if we get OOTE (or any other exception) from backup
      ValueMatcher originalMatcher = command.getValueMatcher();
      // Ignore the previous value on the backup owners
      command.setValueMatcher(ValueMatcher.MATCH_ALWAYS);
      RpcOptions rpcOptions = determineRpcOptionsForBackupReplication(rpcManager, isSynchronous(command), recipients);
      CompletableFuture<Map<Address, Response>> remoteInvocation =
            rpcManager.invokeRemotelyAsync(recipients, command, rpcOptions);
      return returnWithAsync(remoteInvocation.handle((responses, t) -> {
         // Switch to the retry policy, in case the primary owner changed and the write already succeeded on the new primary
         command.setValueMatcher(originalMatcher.matcherForRetry());
         CompletableFutures.rethrowException(t instanceof RemoteException ? t.getCause() : t);
         return localResult;
      }));
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
   public BasicInvocationStage visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         return invokeNext(ctx, command);
      }

      if (ctx.isOriginLocal()) {
         CacheTopology cacheTopology = checkTopologyId(command);
         ConsistentHash ch = cacheTopology.getReadConsistentHash();

         // At this point, we know that an entry located on this node that exists in the data container/store
         // must also exist in the context.
         Map<Address, List<Object>> requestedKeys = new HashMap<>();
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null) {
               Address primaryOwner = ch.locatePrimaryOwner(key);
               requestedKeys.computeIfAbsent(primaryOwner, po -> new ArrayList<>()).add(key);
            }
         }

         if (requestedKeys.isEmpty()) {
            return invokeNext(ctx, command);
         }

         GlobalTransaction gtx = ctx.isInTxScope() ? ((TxInvocationContext) ctx).getGlobalTransaction() : null;
         CompletableFutureWithCounter allFuture = new CompletableFutureWithCounter(requestedKeys.size());

         for (Map.Entry<Address, List<Object>> pair : requestedKeys.entrySet()) {
            List<Object> keys = pair.getValue();
            ClusteredGetAllCommand clusteredGetAllCommand = cf.buildClusteredGetAllCommand(keys, command.getFlagsBitSet(), gtx);
            rpcManager.invokeRemotelyAsync(Collections.singleton(pair.getKey()), clusteredGetAllCommand, defaultSyncOptions).whenComplete((responseMap, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               }
               Response response = getSingleSuccessfulResponseOrFail(responseMap, allFuture);
               if (response == null) return;
               Object responseValue = ((SuccessfulResponse) response).getResponseValue();
               if (responseValue instanceof InternalCacheValue[]) {
                  InternalCacheValue[] values = (InternalCacheValue[]) responseValue;
                  int counterValue;
                  synchronized (allFuture) {
                     for (int i = 0; i < keys.size(); ++i) {
                        Object key = keys.get(i);
                        InternalCacheValue value = values[i];
                        CacheEntry entry = value == null ? NullCacheEntry.getInstance() : value.toInternalCacheEntry(key);
                        entryFactory.wrapExternalEntry(ctx, key, entry, false, false);
                     }
                     counterValue = --allFuture.counter;
                  }
                  // complete the future after sync block!
                  if (counterValue == 0) {
                     allFuture.complete(null);
                  }
               } else {
                  allFuture.completeExceptionally(new IllegalStateException("Unexpected response value: " + responseValue));
               }
            });
         }
         return invokeNextAsync(ctx, command, allFuture);
      } else { // remote
         if (missingKeysInContext(ctx, command.getKeys())) {
            return returnWith(UnsuccessfulResponse.INSTANCE);
         }
         return invokeNext(ctx, command);
      }
   }

   @Override
   public BasicInvocationStage visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      // We cannot merge this method with visitGetAllCommand because this can't wrap entries into context
      if (command.hasFlag(Flag.CACHE_MODE_LOCAL) || command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)) {
         return invokeNext(ctx, command);
      }

      if (ctx.isOriginLocal()) {
         CacheTopology cacheTopology = checkTopologyId(command);
         if (command.getKeys().isEmpty()) {
            return returnWith(Stream.empty());
         }

         ConsistentHash ch = cacheTopology.getReadConsistentHash();
         int estimateForOneNode = 2 * command.getKeys().size() / ch.getMembers().size();
         Map<Address, List<Object>> requestedKeys = new HashMap<>(ch.getMembers().size());
         List<Object> availableKeys = new ArrayList<>(estimateForOneNode);
         for (Object key : command.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(key);
            if (entry == null) {
               List<Address> owners = ch.locateOwners(key);
               // Let's try to minimize the number of messages by preferring owner to which we've already
               // decided to send message
               boolean foundExisting = false;
               for (Address address : owners) {
                  if (address.equals(rpcManager.getAddress())) {
                     throw new IllegalStateException("Entry should be always wrapped!");
                  }
                  List<Object> keys = requestedKeys.get(address);
                  if (keys != null) {
                     keys.add(key);
                     foundExisting = true;
                     break;
                  }
               }
               if (!foundExisting) {
                  List<Object> keys = new ArrayList<>(estimateForOneNode);
                  keys.add(key);
                  requestedKeys.put(owners.get(0), keys);
               }
            } else {
               availableKeys.add(key);
            }
         }

         // TODO: while this works in a non-blocking way, the returned stream is not lazy as the functional
         // contract suggests. Traversable is also not honored as it is executed only locally on originator.
         // On FutureMode.ASYNC, there should be one command per target node going from the top level
         // to allow retries in StateTransferInterceptor in case of topology change.
         MergingCompletableFuture<Object> allFuture = new MergingCompletableFuture<>(
            ctx, requestedKeys.size() + (availableKeys.isEmpty() ? 0 : 1),
            new Object[command.getKeys().size()], Arrays::stream);
         int pos = 0;
         if (!availableKeys.isEmpty()) {
            ReadOnlyManyCommand localCommand = cf.buildReadOnlyManyCommand(availableKeys, command.getFunction());
            invokeNext(ctx, localCommand).compose((stage, rCtx, rCommand, rv, throwable) -> {
               if (throwable != null) {
                  allFuture.completeExceptionally(throwable);
               } else {
                  try {
                     Supplier<ArrayIterator> supplier = () -> new ArrayIterator(allFuture.results);
                     BiConsumer<ArrayIterator, Object> consumer = ArrayIterator::add;
                     BiConsumer<ArrayIterator, ArrayIterator> combiner = ArrayIterator::combine;
                     ((Stream) rv).collect(supplier, consumer, combiner);
                     allFuture.countDown();
                  } catch (Throwable t) {
                     allFuture.completeExceptionally(t);
                  }
               }
               return returnWithAsync(allFuture);
            });
            pos += availableKeys.size();
         }
         for (Map.Entry<Address, List<Object>> addressKeys : requestedKeys.entrySet()) {
            List<Object> keys = addressKeys.getValue();
            ReadOnlyManyCommand remoteCommand = cf.buildReadOnlyManyCommand(keys, command.getFunction());
            final int myPos = pos;
            pos += keys.size();
            rpcManager.invokeRemotelyAsync(Collections.singleton(addressKeys.getKey()), remoteCommand, defaultSyncOptions)
               .whenComplete((responseMap, throwable) -> {
                  if (throwable != null) {
                     allFuture.completeExceptionally(throwable);
                  }
                  Response response = getSingleSuccessfulResponseOrFail(responseMap, allFuture);
                  if (response == null) return;
                  Object responseValue = ((SuccessfulResponse) response).getResponseValue();
                  if (responseValue instanceof Object[]) {
                     Object[] values = (Object[]) responseValue;
                     System.arraycopy(values, 0, allFuture.results, myPos, values.length);
                     allFuture.countDown();
                  } else {
                     allFuture.completeExceptionally(new IllegalStateException("Unexpected response value " + responseValue));
                  }
               });
         }
         return returnWithAsync(allFuture);
      } else { // remote
         if (missingKeysInContext(ctx, command.getKeys())) {
            return returnWith(UnsuccessfulResponse.INSTANCE);
         }
         return invokeNext(ctx, command).thenApply((rCtx, rCommand, rv) -> {
            // apply function happens here
            return ((Stream) rv).toArray();
         });
      }
   }

   protected Response getSingleSuccessfulResponseOrFail(Map<Address, Response> responseMap, CompletableFuture<?> future) {
      Iterator<Response> it = responseMap.values().iterator();
      if (!it.hasNext()) {
         future.completeExceptionally(new RpcException("Expected one response"));
         return null;
      } else {
         Response response = it.next();
         if (it.hasNext()) {
            future.completeExceptionally(new IllegalStateException("Too many responses " + responseMap));
            return null;
         }
         if (!response.isSuccessful()) {
            // CHECKME: The command is sent with current topology and deferred until the node gets our topology;
            // therefore if it returns unsuccessful response we can assume that there is a newer topology
            future.completeExceptionally(new OutdatedTopologyException("Remote node has higher topology, response " + response));
            return null;
         }
         return response;
      }
   }

   private static class ArrayIterator {
      private final Object[] array;
      private int pos = 0;

      public ArrayIterator(Object[] array) {
         this.array = array;
      }

      public void add(Object item) {
         array[pos] = item;
         ++pos;
      }

      public void combine(ArrayIterator other) {
         throw new UnsupportedOperationException("The stream is not supposed to be parallel");
      }
   }

   // This class sis very similar to CountDownCompletableFuture but it expect external synchronization,
   // That comes handy when we have to sync anyway on different item, for example the context
   private static class CompletableFutureWithCounter extends CompletableFuture<Void> {
      private int counter;

      public CompletableFutureWithCounter(int counter) {
         this.counter = counter;
      }
   }

   protected static class CountDownCompletableFuture extends CompletableFuture<Object> {
      protected final InvocationContext ctx;
      protected final AtomicInteger counter;

      public CountDownCompletableFuture(InvocationContext ctx, int participants) {
         if (trace) log.tracef("Creating shortcut countdown with %d participants", participants);
         this.ctx = ctx;
         this.counter = new AtomicInteger(participants);
      }

      public void countDown() {
         if (counter.decrementAndGet() == 0) {
            complete(result());
         }
      }

      public void increment() {
         int preValue = counter.getAndIncrement();
         if (preValue == 0) {
            throw new IllegalStateException();
         }
      }

      protected Object result() {
         return null;
      }
   }

   protected static class MergingCompletableFuture<T> extends CountDownCompletableFuture {
      private final Function<T[], Object> transform;
      protected final T[] results;

      public MergingCompletableFuture(InvocationContext ctx, int participants, T[] results, Function<T[], Object> transform) {
         super(ctx, participants);
         // results can be null if the command has flag IGNORE_RETURN_VALUE
         this.results = results;
         this.transform = transform;
      }

      @Override
      protected Object result() {
         return transform == null || results == null ? null : transform.apply(results);
      }
   }

   private BasicInvocationStage visitGetCommand(InvocationContext ctx, AbstractDataCommand command) throws Throwable {
      Object key = command.getKey();
      CacheEntry entry = ctx.lookupEntry(key);

      if (entry == null) {
         if (ctx.isOriginLocal()) {
            if (readNeedsRemoteValue(ctx, command)) {
               return invokeNextAsync(ctx, command, remoteGet(ctx, command, false));
            }
         } else {
            CacheTopology cacheTopology = checkTopologyId(command);
            if (!cacheTopology.getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key)) {
               if (trace) log.tracef("In topology %d this (%s) is not an owner of %s, owners are %s",
                  cacheTopology.getTopologyId(), rpcManager.getAddress(), key, cacheTopology.getReadConsistentHash().locateOwners(key));
               return returnWith(UnsuccessfulResponse.INSTANCE);
            }
         }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable {
      return visitGetCommand(ctx, command);
   }

   @Override
   public BasicInvocationStage visitGetCacheEntryCommand(InvocationContext ctx,
         GetCacheEntryCommand command) throws Throwable {
      return visitGetCommand(ctx, command);
   }

   protected void wrapMissingWrittenKeysAsNull(InvocationContext ctx, Collection<Object> keys, ConsistentHash ch, Address localAddress) {
      // It would be possible to move this iteration to EWI, but we want EWI to use only readCH
      for (Object key : keys) {
         if (ctx.lookupEntry(key) == null && ch.isKeyLocalToNode(localAddress, key)) {
            entryFactory.wrapExternalEntry(ctx, key, null, true, true);
         }
      }
   }

   protected CacheTopology checkTopologyId(TopologyAffectedCommand command) {
      CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (currentTopologyId != cmdTopology && cmdTopology != -1) {
         throw new OutdatedTopologyException("Cache topology changed while the command was executing: expected " +
            cmdTopology + ", got " + currentTopologyId);
      }
      if (trace) {
         log.tracef("Current topology %d, command topology %d", currentTopologyId, cmdTopology);
      }
      return cacheTopology;
   }

   /**
    * @return {@code true} if the value is not available on the local node and a read command is allowed to
    * fetch it from a remote node. Does not check if the value is already in the context.
    */
   protected boolean readNeedsRemoteValue(InvocationContext ctx, AbstractDataCommand command) {
      return ctx.isOriginLocal() && !command.hasFlag(Flag.CACHE_MODE_LOCAL) &&
            !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP);
   }

   private boolean missingKeysInContext(InvocationContext ctx, Collection<?> keys) {
      for (Object key : keys) {
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry == null) {
            // Two possibilities:
            // a) this node already lost the entry -> we should retry on originator
            // b) the command was wrapped in old topology but now we have the entry -> retry locally
            // TODO: to simplify things, retrying on originator all the time
            return true;
         }
      }
      return false;
   }
}
