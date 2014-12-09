package org.infinispan.statetransfer;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseStateTransferInterceptor;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

//todo [anistor] command forwarding breaks the rule that we have only one originator for a command. this opens now the possibility to have two threads processing incoming remote commands for the same TX
/**
 * This interceptor has two tasks:
 * <ol>
 *    <li>If the command's topology id is higher than the current topology id,
 *    wait for the node to receive transaction data for the new topology id.</li>
 *    <li>If the topology id changed during a command's execution, forward the command to the new owners.</li>
 * </ol>
 *
 * Note that we don't keep track of old cache topologies (yet), so we actually forward the command to all the owners
 * -- not just the ones added in the new topology. Transactional commands are idempotent, so this is fine.
 *
 * In non-transactional mode, the semantic of these tasks changes:
 * <ol>
 *    <li>We don't have transaction data, so the interceptor only waits for the new topology to be installed.</li>
 *    <li>Instead of forwarding commands from any owner, we retry the command on the originator.</li>
 * </ol>
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferInterceptor extends BaseStateTransferInterceptor {

   private static final Log log = LogFactory.getLog(StateTransferInterceptor.class);
   private static boolean trace = log.isTraceEnabled();

   private StateTransferManager stateTransferManager;
   private Transport transport;
   private ComponentRegistry componentRegistry;

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferManager stateTransferManager, Transport transport,
         ComponentRegistry componentRegistry) {
      this.stateTransferManager = stateTransferManager;
      this.transport = transport;
      this.componentRegistry = componentRegistry;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
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
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      return handleNonTxWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      // no need to forward this command
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      // it's not necessary to propagate eviction to the new owners in case of state transfer
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }
      CacheTopology beginTopology = stateTransferManager.getCacheTopology();
      command.setConsistentHashAndAddress(beginTopology.getReadConsistentHash(),
            transport.getAddress());
      Map<Object, Object> values = (Map<Object, Object>) invokeNextInterceptor(ctx, command);
      
      if (ctx.isOriginLocal()) {
         CacheTopology afterTopology = stateTransferManager.getCacheTopology();
         if (beginTopology.getTopologyId() != afterTopology.getTopologyId()) {
            Map<Object, ?> remotelyRetrieved = command.getRemotelyFetched();
            // Locally we have to see if anything is null, this could have meant we had
            // a miss right when a topology changed - so find them and retry!
            Collection<?> originalKeys = command.getKeys();
            List<Object> keysToTryAgain = new ArrayList<Object>(originalKeys.size());
            for (Object key : originalKeys) {
               if (values.containsKey(key)) {
                  Object value = values.get(key);
                  if (value == null && !remotelyRetrieved.containsKey(key)) {
                     // If the value was returned as null and it wasn't a remote lookup
                     // that means it was a possible rehash miss, so we have to look
                     // it up again
                     keysToTryAgain.add(key);
                  }
               } else {
                  // This occurs if a remote miss had a rehash while processing
                  keysToTryAgain.add(key);
               }
            }
            if (!keysToTryAgain.isEmpty()) {
               try {
                  log.tracef("Retrying keys %s", keysToTryAgain);
                  command.setKeys(keysToTryAgain);
                  values.putAll((Map<Object, Object>) visitGetAllCommand(ctx, command));
               } finally {
                  command.setKeys(originalKeys);
               }
            }
         } else {
            Collection<?> originalKeys = command.getKeys();
            int missingKeys;
            // If these don't match it means that a remote node saw a new topology and
            // didn't have the value but our local node hasn't yet seen the new topology
            if ((missingKeys = originalKeys.size() - values.size()) > 0) {
               List<Object> keysToTryAgain = new ArrayList<Object>(missingKeys);
               for (Object key : originalKeys) {
                  if (!values.containsKey(key)) {
                     keysToTryAgain.add(key);
                  }
               }
               if (!keysToTryAgain.isEmpty()) {
                  try {
                     log.infof("Retrying keys %s from stable topology", keysToTryAgain);
                     command.setKeys(keysToTryAgain);
                     // Add in a random sleep here just to reduce the chance of getting
                     // StackOverflow if it takes too long for a suspected node to be
                     // removed from the view
                     Thread.sleep(10);
                     values.putAll((Map<Object, Object>) visitGetAllCommand(ctx, command));
                  } finally {
                     command.setKeys(originalKeys);
                  }
               }
            }
         }
      } else {
         // REMEMBER: remote are always returning InternalCacheEntries
         if (beginTopology.getTopologyId() != stateTransferManager.getCacheTopology().getTopologyId()) {
            // If the topology before and after don't match we can't know for certain if the
            // misses were real or not so don't send those entries back
            Iterator<Entry<Object, Object>> it = values.entrySet().iterator();
            while (it.hasNext()) {
               Entry<Object, Object> entry = it.next();
               InternalCacheEntry ice = (InternalCacheEntry)entry.getValue();
               if (ice == null) {
                  it.remove();
               }
            }
         } else {
            ConsistentHash beginHash = beginTopology.getReadConsistentHash();
            Address localAddress = transport.getAddress();
            // It is possible we were sent a request for a key when we were the read owner
            // but now we aren't
            Iterator<Entry<Object, Object>> it = values.entrySet().iterator();
            while (it.hasNext()) {
               Entry<Object, Object> entry = it.next();
               InternalCacheEntry ice = (InternalCacheEntry)entry.getValue();
               if (ice == null) {
                  // If the key wasn't local or the server is now being shut down
                  // we can't trust the null value
                  if (!beginHash.isKeyLocalToNode(localAddress, entry.getKey()) ||
                        componentRegistry.getStatus() != ComponentStatus.RUNNING) {
                     it.remove();
                  }
               }
            }
         }
      }
      return values;
   }

   /**
    * Special processing required for transaction commands.
    *
    */
   private Object handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      // For local commands we may not have a GlobalTransaction yet
      Address address = ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
      if (trace) log.tracef("handleTxCommand for command %s, origin %s", command, address);

      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }
      updateTopologyId(command);

      int retryTopologyId = -1;
      Object localResult = null;
      try {
         localResult = invokeNextInterceptor(ctx, command);
      } catch (OutdatedTopologyException e) {
         // This can only happen on the originator
         retryTopologyId = command.getTopologyId() + 1;
      }

      if (currentTopologyId() > command.getTopologyId()) {
         retryTopologyId = currentTopologyId();
      }

      boolean async = isTxCommandAsync(command);
      if (async) {
         // We still need to forward the command to the new owners, if the command was asynchronous
         stateTransferManager.forwardCommandIfNeeded(command, getAffectedKeys(ctx, command), address, false);
      } else if (retryTopologyId > 0) {
         // If the command was synchronous, we just retry on the originator
         if (ctx.isOriginLocal()) {
            // Only the originator can retry the command
            command.setTopologyId(retryTopologyId);
            waitForTransactionData(retryTopologyId);

            log.tracef("Retrying command %s for topology %d", command, retryTopologyId);
            handleTxCommand(ctx, command);
         } else {
            // Signal the originator to retry
            return UnsureResponse.INSTANCE;
         }
      }
      return localResult;
   }

   private boolean isTxCommandAsync(TransactionBoundaryCommand command) {
      boolean async = false;
      if (command instanceof CommitCommand || command instanceof RollbackCommand) {
         async = !cacheConfiguration.transaction().syncCommitPhase();
      } else if (command instanceof PrepareCommand) {
         async = !cacheConfiguration.clustering().cacheMode().isSynchronous();
      }
      return async;
   }

   /**
    * For non-tx write commands, we retry the command locally if the topology changed, instead of forwarding to the
    * new owners like we do for tx commands. But we only retry on the originator, and only if the command doesn't have
    * the {@code CACHE_MODE_LOCAL} flag.
    */
   private Object handleNonTxWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (trace) log.tracef("handleNonTxWriteCommand for command %s, topology id %d", command, command.getTopologyId());

      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }

      updateTopologyId(command);

      // Only catch OutdatedTopologyExceptions on the originator
      if (!ctx.isOriginLocal()) {
         return invokeNextInterceptor(ctx, command);
      }

      int commandTopologyId = command.getTopologyId();
      Object localResult;
      try {
         localResult = invokeNextInterceptor(ctx, command);
         return localResult;
      } catch (CacheException e) {
         Throwable ce = e;
         while (ce instanceof RemoteException) {
            ce = ce.getCause();
         }
         if (!(ce instanceof OutdatedTopologyException) && !(ce instanceof SuspectException))
            throw e;

         // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the next topology.
         // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
         if (trace) log.tracef("Retrying command because of topology change, current topology is %d: %s", command);
         int newTopologyId = Math.max(currentTopologyId(), commandTopologyId + 1);
         command.setTopologyId(newTopologyId);
         waitForTransactionData(newTopologyId);

         command.setFlags(Flag.COMMAND_RETRY);
         localResult = handleNonTxWriteCommand(ctx, command);
      }

      // We retry the command every time the topology changes, either in NonTxConcurrentDistributionInterceptor or in
      // EntryWrappingInterceptor. So we don't need to forward the command again here (without holding a lock).
      // stateTransferManager.forwardCommandIfNeeded(command, command.getAffectedKeys(), ctx.getOrigin(), false);
      return localResult;
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (command instanceof TopologyAffectedCommand) {
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin());
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private Object handleTopologyAffectedCommand(InvocationContext ctx, VisitableCommand command,
                                                Address origin) throws Throwable {
      if (trace) log.tracef("handleTopologyAffectedCommand for command %s, origin %s", command, origin);

      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }
      updateTopologyId((TopologyAffectedCommand) command);

      return invokeNextInterceptor(ctx, command);
   }

   private Object mergeResponses(Map<Address, Response> responseMap, Object localResult, InvocationContext context,
                                 VisitableCommand command) {
      if (command instanceof VersionedPrepareCommand) {
         return mergeVersionedPrepareCommand(responseMap, (TxInvocationContext) context, localResult);
      }
      return localResult;
   }

   private Object mergeVersionedPrepareCommand(Map<Address, Response> responseMap,
                                               TxInvocationContext txInvocationContext, Object localResult) {
      if (txInvocationContext.isOriginLocal()) {
         final CacheTransaction cacheTransaction = txInvocationContext.getCacheTransaction();
         for (Response response : responseMap.values()) {
            if (response != null && response.isSuccessful()) {
               SuccessfulResponse sr = (SuccessfulResponse) response;
               EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
               if (uv != null) {
                  cacheTransaction.setUpdatedEntryVersions(uv.merge(cacheTransaction.getUpdatedEntryVersions()));
               }
            }
         }
         return localResult;
      } else {
         EntryVersionsMap mergeResult = localResult instanceof EntryVersionsMap ? (EntryVersionsMap) localResult :
               new EntryVersionsMap();
         for (Response response : responseMap.values()) {
            if (response != null && response.isSuccessful()) {
               SuccessfulResponse sr = (SuccessfulResponse) response;
               EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
               if (uv != null) {
                  mergeResult = mergeResult.merge(uv);
               }
            }
         }
         return mergeResult;
      }
   }

   private boolean isLocalOnly(InvocationContext ctx, VisitableCommand command) {
      boolean transactionalWrite = ctx.isInTxScope() && command instanceof WriteCommand;
      boolean cacheModeLocal = false;
      if (command instanceof FlagAffectedCommand) {
         cacheModeLocal = ((FlagAffectedCommand)command).hasFlag(Flag.CACHE_MODE_LOCAL);
      }
      return cacheModeLocal || transactionalWrite;
   }

   @SuppressWarnings("unchecked")
   private Set<Object> getAffectedKeys(InvocationContext ctx, VisitableCommand command) {
      Set<Object> affectedKeys = null;
      try {
         affectedKeys = (Set<Object>) command.acceptVisitor(ctx, affectedKeysVisitor);
      } catch (Throwable throwable) {
         // impossible to reach this
      }
      if (affectedKeys == null) {
         affectedKeys = InfinispanCollections.emptySet();
      }
      return affectedKeys;
   }
}
