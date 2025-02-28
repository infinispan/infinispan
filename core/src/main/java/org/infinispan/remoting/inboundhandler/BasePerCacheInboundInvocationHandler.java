package org.infinispan.remoting.inboundhandler;

import static org.infinispan.factories.KnownComponentNames.BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation with the default handling methods and utilities methods.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class BasePerCacheInboundInvocationHandler implements PerCacheInboundInvocationHandler {
   private static final Log log = LogFactory.getLog(BasePerCacheInboundInvocationHandler.class);

   private static final int NO_TOPOLOGY_COMMAND = Integer.MIN_VALUE;

   // TODO: To be removed with https://issues.redhat.com/browse/ISPN-11483
   @Inject @ComponentName(BLOCKING_EXECUTOR)
   protected BlockingTaskAwareExecutorService blockingExecutor;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR)
   protected BlockingTaskAwareExecutorService nonBlockingExecutor;
   @Inject StateTransferLock stateTransferLock;
   @Inject ResponseGenerator responseGenerator;
   @Inject ComponentRegistry componentRegistry;
   @Inject protected Configuration configuration;
   // Stop after RpcManager, so we stop accepting requests before we are unable to send requests ourselves
   @Inject RpcManager rpcManager;

   private volatile boolean stopped;
   private volatile int firstTopologyAsMember = Integer.MAX_VALUE;

   static int extractCommandTopologyId(CacheRpcCommand command) {
      if (command instanceof SingleRpcCommand rpc)
         return topology(rpc.getCommand());

      if (command instanceof ClusteredGetCommand || command instanceof ClusteredGetAllCommand) {
         // These commands are topology aware but we don't block them here - topologyId logic
         // is handled in StateTransferInterceptor
         return NO_TOPOLOGY_COMMAND;
      }
      return topology(command);
   }

   private static int topology(ReplicableCommand cmd) {
      return cmd instanceof TopologyAffectedCommand tac ? tac.getTopologyId() : NO_TOPOLOGY_COMMAND;
   }

   @Start
   public void start() {
      this.stopped = false;
   }

   @Stop
   public void stop() {
      this.stopped = true;
   }

   public boolean isStopped() {
      return stopped;
   }

   final CompletableFuture<Response> invokeCommand(CacheRpcCommand cmd) throws Throwable {
      if (log.isTraceEnabled()) {
         log.tracef("Calling invokeAsync() on %s", cmd);
      }
      CompletableFuture<?> future = cmd.invokeAsync(componentRegistry).toCompletableFuture();
      if (CompletionStages.isCompletedSuccessfully(future)) {
         Object obj = future.join();
         Response response = responseGenerator.getResponse(cmd, obj);
         if (response == null) {
            return CompletableFutures.completedNull();
         }
         return CompletableFuture.completedFuture(response);
      }
      return future.handle((rv, throwable) -> {
         CompletableFutures.rethrowExceptionIfPresent(throwable);

         return responseGenerator.getResponse(cmd, rv);
      });
   }

   final StateTransferLock getStateTransferLock() {
      return stateTransferLock;
   }

   static ExceptionResponse exceptionHandlingCommand(CacheRpcCommand command, Throwable throwable) {
      if (command.logThrowable(throwable)) {
         CLUSTER.exceptionHandlingCommand(command, throwable);
      }
      if (throwable instanceof Exception) {
         return new ExceptionResponse(((Exception) throwable));
      } else {
         return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
      }
   }

   static ExceptionResponse outdatedTopology(OutdatedTopologyException exception) {
      log.tracef("Topology changed, retrying: %s", exception);
      return new ExceptionResponse(exception);
   }

   static Response interruptedException(CacheRpcCommand command) {
      CLUSTER.debugf("Shutdown while handling command %s", command);
      return CacheNotFoundResponse.INSTANCE;
   }

   final void handleRunnable(BlockingRunnable runnable, boolean onExecutorService) {
      // This means it is blocking and not preserve order per executeOnExecutorService
      if (onExecutorService) {
         blockingExecutor.execute(runnable);
      } else {
         runnable.run();
      }
   }

   public final boolean isCommandSentBeforeFirstTopology(int commandTopologyId) {
      if (0 <= commandTopologyId && commandTopologyId < firstTopologyAsMember) {
         if (log.isTraceEnabled()) {
            log.tracef("Ignoring command sent before the local node was a member (command topology id is %d, first topology as member is %d)", commandTopologyId, firstTopologyAsMember);
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

   static boolean executeOnExecutorService(DeliverOrder order, CacheRpcCommand command) {
      return !order.preserveOrder() && command.canBlock();
   }

   @Override
   public void setFirstTopologyAsMember(int firstTopologyAsMember) {
      this.firstTopologyAsMember = firstTopologyAsMember;
   }

   @Override
   public int getFirstTopologyAsMember() {
         return firstTopologyAsMember;
   }

   @Override
   public void checkForReadyTasks() {
      blockingExecutor.checkForReadyTasks();
      nonBlockingExecutor.checkForReadyTasks();
   }
}
