package org.infinispan.topology;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.topology.AbstractCacheControlCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class TopologyManagementHelper {
   private static final Log log = LogFactory.getLog(TopologyManagementHelper.class);

   private GlobalComponentRegistry gcr;
   private BasicComponentRegistry bcr;

   public TopologyManagementHelper(GlobalComponentRegistry gcr) {
      this.gcr = gcr;
      this.bcr = gcr.getComponent(BasicComponentRegistry.class);
   }

   public <T> CompletionStage<T> executeOnClusterSync(Transport transport, ReplicableCommand command,
         int timeout, ResponseCollector<T> responseCollector) {
      // First invoke the command remotely, but make sure we don't call finish() on the collector
      ResponseCollector<Void> delegatingCollector = new DelegatingResponseCollector<>(responseCollector);
      CompletionStage<Void> remoteFuture =
            transport.invokeCommandOnAll(command, delegatingCollector, DeliverOrder.NONE, timeout, MILLISECONDS);

      // Then invoke the command on the local node
      CompletionStage<?> localFuture;
      try {
         if (log.isTraceEnabled())
            log.tracef("Attempting to execute command on self: %s", command);
         bcr.wireDependencies(command, true);
         localFuture = invokeAsync(command);
      } catch (Throwable throwable) {
         localFuture = CompletableFuture.failedFuture(throwable);
      }

      return addLocalResult(responseCollector, remoteFuture, localFuture, transport.getAddress());
   }

   public void executeOnClusterAsync(Transport transport, ReplicableCommand command) {
      // invoke remotely
      try {
         DeliverOrder deliverOrder = DeliverOrder.NONE;
         transport.sendToAll(command, deliverOrder);
      } catch (Exception e) {
         throw Util.rewrapAsCacheException(e);
      }

      // invoke the command on the local node
      try {
         if (log.isTraceEnabled())
            log.tracef("Attempting to execute command on self: %s", command);
         bcr.wireDependencies(command, true);
         invokeAsync(command);
      } catch (Throwable throwable) {
         // The command already logs any exception in invoke()
      }
   }

   public CompletionStage<Object> executeOnCoordinator(Transport transport, ReplicableCommand command,
                                                       long timeoutMillis) {
      CompletionStage<? extends Response> responseStage;
      Address coordinator = transport.getCoordinator();
      if (transport.getAddress().equals(coordinator)) {
         try {
            if (log.isTraceEnabled())
               log.tracef("Attempting to execute command on self: %s", command);
            bcr.wireDependencies(command, true);
            responseStage = invokeAsync(command).thenApply(v -> makeResponse(v, null, transport.getAddress()));
         } catch (Throwable t) {
            throw CompletableFutures.asCompletionException(t);
         }
      } else {
         // this node is not the coordinator
         responseStage = transport.invokeCommand(coordinator, command, SingleResponseCollector.validOnly(),
                                                 DeliverOrder.NONE, timeoutMillis, TimeUnit.MILLISECONDS);
      }
      return responseStage.thenApply(response -> {
         if (!(response instanceof SuccessfulResponse)) {
            throw CLUSTER.unexpectedResponse(coordinator, response);
         }
         return ((SuccessfulResponse) response).getResponseValue();
      });
   }

   public void executeOnCoordinatorAsync(Transport transport, AbstractCacheControlCommand command) throws Exception {
      if (transport.isCoordinator()) {
         if (log.isTraceEnabled())
            log.tracef("Attempting to execute command on self: %s", command);
         try {
            // ignore the result
            invokeAsync(command);
         } catch (Throwable t) {
            log.errorf(t, "Failed to execute ReplicableCommand %s on coordinator async: %s", command, t.getMessage());
         }
      } else {
         Address coordinator = transport.getCoordinator();
         // ignore the response
         transport.sendTo(coordinator, command, DeliverOrder.NONE);
      }
   }

   private <T> CompletionStage<T> addLocalResult(ResponseCollector<T> responseCollector,
                                                 CompletionStage<Void> remoteFuture,
                                                 CompletionStage<?> localFuture, Address localAddress) {
      return remoteFuture.thenCompose(ignore -> localFuture.handle((v, t) -> {
         Response localResponse = makeResponse(v, t, localAddress);

         // No more responses are coming, so we don't need to synchronize
         responseCollector.addResponse(localAddress, localResponse);
         return responseCollector.finish();
      }));
   }

   private Response makeResponse(Object v, Throwable t, Address localAddress) {
      Response localResponse;
      if (t != null) {
         localResponse = new ExceptionResponse(
               CLUSTER.remoteException(localAddress, CompletableFutures.extractException(t)));
      } else {
         if (v instanceof Response) {
            localResponse = ((Response) v);
         } else {
            localResponse = SuccessfulResponse.create(v);
         }
      }
      return localResponse;
   }

   private CompletionStage<?> invokeAsync(ReplicableCommand command) throws Throwable {
      if (command instanceof GlobalRpcCommand)
         return ((GlobalRpcCommand) command).invokeAsync(gcr);
      return command.invokeAsync();
   }

   private static class DelegatingResponseCollector<T> implements ResponseCollector<Void> {
      private final ResponseCollector<T> responseCollector;

      public DelegatingResponseCollector(ResponseCollector<T> responseCollector) {
         this.responseCollector = responseCollector;
      }

      @Override
      public Void addResponse(Address sender, Response response) {
         responseCollector.addResponse(sender, response);
         return null;
      }

      @Override
      public Void finish() {
         return null;
      }
   }
}
