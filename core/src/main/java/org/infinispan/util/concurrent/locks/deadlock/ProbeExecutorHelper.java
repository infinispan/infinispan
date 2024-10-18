package org.infinispan.util.concurrent.locks.deadlock;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

final class ProbeExecutorHelper {

   private static final Log log = LogFactory.getLog(ProbeExecutorHelper.class);

   private final RpcManager transport;
   private final RpcOptions options;
   private final ComponentRegistry cr;

   ProbeExecutorHelper(ComponentRegistry cr) {
      this.cr = cr;
      this.transport = cr.getRpcManager().running();

      Configuration configuration = cr.getConfiguration();
      this.options = new RpcOptions(DeliverOrder.NONE, configuration.locking().lockAcquisitionTimeout(), TimeUnit.MILLISECONDS);
   }

   public CompletionStage<Void> execute(Address address, DeadlockProbeCommand command) {
      // The transport does not invoke on self. Make sure we run the command locally.
      if (address.equals(transport.getAddress())) {
         try {
            return command.invokeAsync(cr)
                  .thenApply(CompletableFutures.toNullFunction());
         } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
         }
      }
      return transport.invokeCommand(address, command, VoidResponseCollector.ignoreLeavers(), options)
            .thenApply(CompletableFutures.toNullFunction());
   }

   public CompletionStage<Void> execute(Collection<Address> targets, DeadlockProbeCommand command) {
      return transport.invokeCommand(targets, command, MapResponseCollector.ignoreLeavers(targets.size()), options)
            .thenApply(res -> {
               for (Map.Entry<Address, Response> entry : res.entrySet()) {
                  Address replier = entry.getKey();
                  Response body = entry.getValue();

                  // A node has a topology update.
                  // We need to try the command again to make sure the remote nodes rollback the TX and mark the locks.
                  if (body == UnsureResponse.INSTANCE) {
                     log.tracef("%s is unsure to probe %s", replier, command);
                     throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
                  }
               }
               return null;
            });
   }

   public Address getLocalAddress() {
      return transport.getAddress();
   }
}
