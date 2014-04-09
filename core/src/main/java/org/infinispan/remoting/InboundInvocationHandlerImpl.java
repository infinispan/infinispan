package org.infinispan.remoting;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.totalorder.RetryPrepareException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   private GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private Transport transport;
   private CancellationService cancelService;
   private BlockingTaskAwareExecutorService remoteCommandsExecutor;
   private BlockingTaskAwareExecutorService totalOrderExecutorService;

   @Inject
   public void inject(GlobalComponentRegistry gcr, Transport transport,
                      @ComponentName(KnownComponentNames.REMOTE_COMMAND_EXECUTOR) BlockingTaskAwareExecutorService remoteCommandsExecutor,
                      @ComponentName(KnownComponentNames.TOTAL_ORDER_EXECUTOR) BlockingTaskAwareExecutorService totalOrderExecutorService,
                      CancellationService cancelService) {
      this.gcr = gcr;
      this.transport = transport;
      this.cancelService = cancelService;
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.totalOrderExecutorService = totalOrderExecutorService;
   }

   @Override
   public void handle(final CacheRpcCommand cmd, Address origin, org.jgroups.blocks.Response response, boolean preserveOrder) throws Throwable {
      cmd.setOrigin(origin);

      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (trace) log.tracef("Silently ignoring that %s cache is not defined", cacheName);
         reply(response, CacheNotFoundResponse.INSTANCE);
         return;
      }

      handleWithWaitForBlocks(cmd, cr, response, preserveOrder);
   }


   private Response handleInternal(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      try {
         if (trace) log.tracef("Calling perform() on %s", cmd);
         ResponseGenerator respGen = cr.getResponseGenerator();
         if(cmd instanceof CancellableCommand){
            cancelService.register(Thread.currentThread(), ((CancellableCommand) cmd).getUUID());
         }
         Object retval = cmd.perform(null);
         Response response = respGen.getResponse(cmd, retval);
         log.tracef("About to send back response %s for command %s", response, cmd);
         return response;
      } catch (Exception e) {
         log.exceptionExecutingInboundCommand(e);
         return new ExceptionResponse(e);
      } finally {
         if(cmd instanceof CancellableCommand){
            cancelService.unregister(((CancellableCommand)cmd).getUUID());
         }
      }
   }

   private void handleWithWaitForBlocks(final CacheRpcCommand cmd, final ComponentRegistry cr, final org.jgroups.blocks.Response response, boolean preserveOrder) throws Throwable {
      final StateTransferManager stm = cr.getStateTransferManager();
      // We must have completed the join before handling commands
      // (even if we didn't complete the initial state transfer)
      if (cmd instanceof TotalOrderPrepareCommand && !stm.ownsData()) {
         reply(response, null);
         return;
      }

      CommandsFactory commandsFactory = cr.getCommandsFactory();

      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd, true);
      if (cmd instanceof TotalOrderPrepareCommand) {
         final TotalOrderRemoteTransactionState state = ((TotalOrderPrepareCommand) cmd).getOrCreateState();
         final TotalOrderManager totalOrderManager = cr.getTotalOrderManager();
         totalOrderManager.ensureOrder(state, ((PrepareCommand) cmd).getAffectedKeysToLock(false));
         totalOrderExecutorService.execute(new BlockingRunnable() {
            @Override
            public boolean isReady() {
               for (TotalOrderLatch block : state.getConflictingTransactionBlocks()) {
                  if (block.isBlocked()) {
                     return false;
                  }
               }
               return true;
            }

            @Override
            public void run() {
               Response resp;
               try {
                  resp = handleInternal(cmd, cr);
               } catch (RetryPrepareException retry) {
                  log.debugf(retry, "Prepare [%s] conflicted with state transfer", cmd);
                  resp = new ExceptionResponse(retry);
               } catch (Throwable throwable) {
                  log.exceptionHandlingCommand(cmd, throwable);
                  resp = new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
               }
               //the ResponseGenerated is null in this case because the return value is a Response
               reply(response, resp);
               if (resp instanceof ExceptionResponse) {
                  totalOrderManager.release(state);
               }
               afterResponseSent(cmd, resp);
            }
         });
      } else {
         final StateTransferLock stateTransferLock = cr.getStateTransferLock();
         // Always wait for the first topology (i.e. for the join to finish)
         final int commandTopologyId = Math.max(extractCommandTopologyId(cmd), 0);

         if (!preserveOrder && cmd.canBlock()) {
            remoteCommandsExecutor.execute(new BlockingRunnable() {
               @Override
               public boolean isReady() {
                  return stateTransferLock.transactionDataReceived(commandTopologyId);
               }

               @Override
               public void run() {
                  if (0 < commandTopologyId && commandTopologyId < stm.getFirstTopologyAsMember()) {
                     if (trace) log.tracef("Ignoring command sent before the local node was a member " +
                           "(command topology id is %d)", commandTopologyId);
                     reply(response, null);
                     return;
                  }
                  Response resp;
                  try {
                     resp = handleInternal(cmd, cr);
                  } catch (Throwable throwable) {
                     log.exceptionHandlingCommand(cmd, throwable);
                     resp = new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
                  }
                  reply(response, resp);
                  afterResponseSent(cmd, resp);
               }
            });
         } else {
            // Non-OOB commands. We still have to wait for transaction data, but we should "never" time out
            // In non-transactional caches, this just waits for the topology to be installed
            stateTransferLock.waitForTransactionData(commandTopologyId, 1, TimeUnit.DAYS);

            if (0 < commandTopologyId && commandTopologyId < stm.getFirstTopologyAsMember()) {
               if (trace) log.tracef("Ignoring command sent before the local node was a member " +
                     "(command topology id is %d)", commandTopologyId);
               reply(response, null);
               return;
            }

            Response resp = handleInternal(cmd, cr);

            // A null response is valid and OK ...
            if (trace && resp != null && !resp.isValid()) {
               // invalid response
               log.tracef("Unable to execute command, got invalid response %s", resp);
            }
            reply(response, resp);
            afterResponseSent(cmd, resp);
         }
      }
   }

   private int extractCommandTopologyId(CacheRpcCommand cmd) {
      int commandTopologyId = -1;
      if (cmd instanceof SingleRpcCommand) {
         ReplicableCommand innerCmd = ((SingleRpcCommand) cmd).getCommand();
         if (innerCmd instanceof TopologyAffectedCommand) {
            commandTopologyId = ((TopologyAffectedCommand) innerCmd).getTopologyId();
         }
      } else if (cmd instanceof MultipleRpcCommand) {
         for (ReplicableCommand innerCmd : ((MultipleRpcCommand) cmd).getCommands()) {
            if (innerCmd instanceof TopologyAffectedCommand) {
               commandTopologyId = Math.max(((TopologyAffectedCommand) innerCmd).getTopologyId(), commandTopologyId);
            }
         }
      } else if (cmd instanceof TopologyAffectedCommand) {
         commandTopologyId = ((TopologyAffectedCommand) cmd).getTopologyId();
      }
      return commandTopologyId;
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal) {
      if (response != null) {
         response.send(retVal, false);
      }
   }

   /**
    * invoked after the {@link Response} is sent back to the originator.
    *
    * @param command the remote command
    * @param resp    the response sent
    */
   private void afterResponseSent(CacheRpcCommand command, Response resp) {
      if (command instanceof TotalOrderCommitCommand ||
            command instanceof TotalOrderVersionedCommitCommand ||
            command instanceof TotalOrderRollbackCommand ||
            (command instanceof TotalOrderPrepareCommand &&
                   (((PrepareCommand) command).isOnePhaseCommit() || resp instanceof ExceptionResponse))) {
         totalOrderExecutorService.checkForReadyTasks();
      }
   }

}

