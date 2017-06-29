package org.infinispan.remoting.inboundhandler;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.inboundhandler.action.ReadyAction;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;

/**
 * Implementation with the default handling methods and utilities methods.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class BasePerCacheInboundInvocationHandler implements PerCacheInboundInvocationHandler {
   private static final int NO_TOPOLOGY_COMMAND = Integer.MIN_VALUE;
   protected BlockingTaskAwareExecutorService remoteCommandsExecutor;
   private StateTransferLock stateTransferLock;
   protected StateTransferManager stateTransferManager;
   private ResponseGenerator responseGenerator;
   private CancellationService cancellationService;
   private volatile boolean stopped = false;

   private static int extractCommandTopologyId(SingleRpcCommand command) {
      ReplicableCommand innerCmd = command.getCommand();
      if (innerCmd instanceof TopologyAffectedCommand) {
         return ((TopologyAffectedCommand) innerCmd).getTopologyId();
      }
      return NO_TOPOLOGY_COMMAND;
   }

   static int extractCommandTopologyId(CacheRpcCommand command) {
      switch (command.getCommandId()) {
         case SingleRpcCommand.COMMAND_ID:
            return extractCommandTopologyId((SingleRpcCommand) command);
         case ClusteredGetCommand.COMMAND_ID:
         case ClusteredGetAllCommand.COMMAND_ID:
            // These commands are topology aware but we don't block them here - topologyId logic
            // is handled in StateTransferInterceptor
            return NO_TOPOLOGY_COMMAND;
         default:
            if (command instanceof TopologyAffectedCommand) {
               return ((TopologyAffectedCommand) command).getTopologyId();
            }
      }
      return NO_TOPOLOGY_COMMAND;
   }

   @Inject
   public void injectDependencies(@ComponentName(REMOTE_COMMAND_EXECUTOR) BlockingTaskAwareExecutorService remoteCommandsExecutor,
                                  ResponseGenerator responseGenerator,
                                  CancellationService cancellationService,
                                  StateTransferLock stateTransferLock,
                                  StateTransferManager stateTransferManager) {
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.responseGenerator = responseGenerator;
      this.cancellationService = cancellationService;
      this.stateTransferLock = stateTransferLock;
      this.stateTransferManager = stateTransferManager;
   }

   @Stop
   public void stop() {
      this.stopped = true;
   }

   public boolean isStopped() {
      return stopped;
   }

   final CompletableFuture<Response> invokeCommand(CacheRpcCommand cmd) throws Throwable {
      try {
         if (isTraceEnabled()) {
            getLog().tracef("Calling perform() on %s", cmd);
         }
         if (cmd instanceof CancellableCommand) {
            cancellationService.register(Thread.currentThread(), ((CancellableCommand) cmd).getUUID());
         }
         CompletableFuture<Object> future = cmd.invokeAsync();
         return future.handle((rv, throwable) -> {
            if (cmd instanceof CancellableCommand) {
               cancellationService.unregister(((CancellableCommand) cmd).getUUID());
            }
            CompletableFutures.rethrowException(throwable);

            return responseGenerator.getResponse(cmd, rv);
         });
      } catch (Throwable throwable) {
         if (cmd instanceof CancellableCommand) {
            cancellationService.unregister(((CancellableCommand) cmd).getUUID());
         }
         throw throwable;
      }
   }

   final StateTransferLock getStateTransferLock() {
      return stateTransferLock;
   }

   final ExceptionResponse exceptionHandlingCommand(CacheRpcCommand command, Throwable throwable) {
      getLog().exceptionHandlingCommand(command, throwable);
      if (throwable instanceof Exception) {
         return new ExceptionResponse(((Exception) throwable));
      } else {
         return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
      }
   }

   final ExceptionResponse outdatedTopology(OutdatedTopologyException exception) {
      getLog().tracef("Topology changed, notifying the originator: %s", exception);
      return new ExceptionResponse(exception);
   }

   final Response interruptedException(CacheRpcCommand command) {
      getLog().shutdownHandlingCommand(command);
      return CacheNotFoundResponse.INSTANCE;
   }

   final void unexpectedDeliverMode(ReplicableCommand command, DeliverOrder deliverOrder) {
      throw new IllegalArgumentException(String.format("Unexpected deliver mode %s for command%s", deliverOrder, command));
   }

   final void handleRunnable(BlockingRunnable runnable, boolean onExecutorService) {
      if (onExecutorService) {
         remoteCommandsExecutor.execute(runnable);
      } else {
         runnable.run();
      }
   }

   public final boolean isCommandSentBeforeFirstTopology(int commandTopologyId) {
      if (0 <= commandTopologyId && commandTopologyId < stateTransferManager.getFirstTopologyAsMember()) {
         if (isTraceEnabled()) {
            getLog().tracef("Ignoring command sent before the local node was a member (command topology id is %d)", commandTopologyId);
         }
         return true;
      }
      return false;
   }

   final BlockingRunnable createDefaultRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
         boolean waitTransactionalData, boolean onExecutorService,
         boolean sync) {
      return new DefaultTopologyRunnable(this, command, reply,
                                         TopologyMode.create(onExecutorService, waitTransactionalData),
                                         commandTopologyId, sync);
   }

   final BlockingRunnable createDefaultRunnable(final CacheRpcCommand command, final Reply reply,
         final int commandTopologyId, TopologyMode topologyMode,
         boolean sync) {
      return new DefaultTopologyRunnable(this, command, reply, topologyMode, commandTopologyId, sync);
   }

   protected abstract Log getLog();

   protected abstract boolean isTraceEnabled();

   final boolean executeOnExecutorService(DeliverOrder order, CacheRpcCommand command) {
      return !order.preserveOrder() && command.canBlock();
   }

   final BlockingRunnable createReadyActionRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
         boolean sync, ReadyAction readyAction) {
      if (readyAction != null) {
         return createNonNullReadyActionRunnable(command, reply, commandTopologyId, sync, readyAction);
      } else {
         return new DefaultTopologyRunnable(this, command, reply, TopologyMode.READY_TX_DATA, commandTopologyId, sync);
      }
   }

   final BlockingRunnable createNonNullReadyActionRunnable(CacheRpcCommand command, Reply reply, int commandTopologyId,
         boolean sync, ReadyAction readyAction) {
      readyAction.addListener(remoteCommandsExecutor::checkForReadyTasks);
      return new DefaultTopologyRunnable(this, command, reply, TopologyMode.READY_TX_DATA, commandTopologyId, sync) {
         @Override
         public boolean isReady() {
            return super.isReady() && readyAction.isReady();
         }

         @Override
         protected void onException(Throwable throwable) {
            super.onException(throwable);
            readyAction.onException();
         }

         @Override
         protected void onFinally() {
            super.onFinally();
            readyAction.onFinally();
         }
      };
   }
}
