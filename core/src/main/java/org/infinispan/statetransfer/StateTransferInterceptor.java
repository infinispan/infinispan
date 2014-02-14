package org.infinispan.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.*;
import org.infinispan.commands.write.*;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
public class StateTransferInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(StateTransferInterceptor.class);
   private static boolean trace = log.isTraceEnabled();

   private StateTransferLock stateTransferLock;

   private StateTransferManager stateTransferManager;

   private CommandsFactory commandFactory;

   private boolean useVersioning;
   private long transactionDataTimeout;

   private final AffectedKeysVisitor affectedKeysVisitor = new AffectedKeysVisitor();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void init(StateTransferLock stateTransferLock, Configuration configuration,
                    CommandsFactory commandFactory, StateTransferManager stateTransferManager) {
      this.stateTransferLock = stateTransferLock;
      this.commandFactory = commandFactory;
      this.stateTransferManager = stateTransferManager;

      useVersioning = configuration.transaction().transactionMode().isTransactional() && configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC && configuration.versioning().enabled();
      transactionDataTimeout = configuration.clustering().sync().replTimeout();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.getCacheTransaction() instanceof RemoteTransaction) {
         ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());
      }

      return handleTxCommand(ctx, command);
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.getCacheTransaction() instanceof RemoteTransaction) {
         // If a commit is received for a transaction that doesn't have its 'lookedUpEntries' populated
         // we know for sure this transaction is 2PC and was received via state transfer but the preceding PrepareCommand
         // was not received by local node because it was executed on the previous key owners. We need to re-prepare
         // the transaction on local node to ensure its locks are acquired and lookedUpEntries is properly populated.
         RemoteTransaction remoteTx = (RemoteTransaction) ctx.getCacheTransaction();
         if (trace) {
            log.tracef("Remote tx topology id %d and command topology is %d", remoteTx.lookedUpEntriesTopology(),
                       command.getTopologyId());
         }
         if (remoteTx.lookedUpEntriesTopology() < command.getTopologyId()) {
            ctx.skipTransactionCompleteCheck(true);
            remoteTx.setLookedUpEntriesTopology(command.getTopologyId());

            PrepareCommand prepareCommand;
            if (useVersioning) {
               prepareCommand = commandFactory.buildVersionedPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
            } else {
               prepareCommand = commandFactory.buildPrepareCommand(ctx.getGlobalTransaction(), ctx.getModifications(), false);
            }
            commandFactory.initializeReplicableCommand(prepareCommand, true);
            prepareCommand.setOrigin(ctx.getOrigin());
            log.tracef("Replaying the transactions received as a result of state transfer %s", prepareCommand);
            prepareCommand.perform(null);
         }
      }

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

   /**
    * Special processing required for transaction commands.
    *
    */
   private Object handleTxCommand(TxInvocationContext ctx, TransactionBoundaryCommand command) throws Throwable {
      // For local commands we may not have a GlobalTransaction yet
      Address address = ctx.isOriginLocal() ? ctx.getOrigin() : ctx.getGlobalTransaction().getAddress();
      return handleTopologyAffectedCommand(ctx, command, address, true);
   }

   /**
    * For non-tx write commands, we retry the command locally if the topology changed, instead of forwarding to the
    * new owners like we do for tx commands. But we only retry on the originator, and only if the command doesn't have
    * the {@code CACHE_MODE_LOCAL} flag.
    */
   private Object handleNonTxWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      log.tracef("handleNonTxWriteCommand for command %s", command);

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

         log.tracef("Retrying command because of topology change: %s", command);
         // We increment the topology id so that updateTopologyIdAndWaitForTransactionData waits for the next topology.
         // Without this, we could retry the command too fast and we could get the OutdatedTopologyException again.
         int newTopologyId = Math.max(stateTransferManager.getCacheTopology().getTopologyId(), commandTopologyId + 1);
         command.setTopologyId(newTopologyId);
         command.setFlags(Flag.COMMAND_RETRY);
         stateTransferLock.waitForTransactionData(newTopologyId, transactionDataTimeout, TimeUnit.MILLISECONDS);
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
         return handleTopologyAffectedCommand(ctx, command, ctx.getOrigin(), true);
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   private Object handleTopologyAffectedCommand(InvocationContext ctx, VisitableCommand command,
                                                Address origin, boolean sync) throws Throwable {
      log.tracef("handleTopologyAffectedCommand for command %s", command);

      if (isLocalOnly(ctx, command)) {
         return invokeNextInterceptor(ctx, command);
      }
      updateTopologyId((TopologyAffectedCommand) command);

      // TODO we may need to skip local invocation for read/write/tx commands if the command is too old and none of its keys are local
      Object localResult = invokeNextInterceptor(ctx, command);

      boolean isNonTransactionalWrite = !ctx.isInTxScope() && command instanceof WriteCommand;
      boolean isTransactionalAndNotRolledBack = false;
      if (ctx.isInTxScope()) {
         isTransactionalAndNotRolledBack = !((TxInvocationContext)ctx).getCacheTransaction().isMarkedForRollback();
      }

      if (isNonTransactionalWrite || isTransactionalAndNotRolledBack) {
         Map<Address, Response> responseMap =  stateTransferManager
               .forwardCommandIfNeeded(((TopologyAffectedCommand) command), getAffectedKeys(ctx, command), origin, sync);
         localResult = mergeResponses(responseMap, localResult, ctx, command);
      }

      return localResult;
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

   private void updateTopologyId(TopologyAffectedCommand command) throws InterruptedException {
      // set the topology id if it was not set before (ie. this is local command)
      // TODO Make tx commands extend FlagAffectedCommand so we can use CACHE_MODE_LOCAL in TransactionTable.cleanupStaleTransactions
      if (command.getTopologyId() == -1) {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         if (cacheTopology != null) {
            command.setTopologyId(cacheTopology.getTopologyId());
         }
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
