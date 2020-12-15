package org.infinispan.xsite.statetransfer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Collects and merges all the {@link StateTransferStatus} from all nodes in the cluster.
 *
 * @author Pedro Ruivo
 * @since 12
 */
class StatusResponseCollector extends ValidResponseCollector<Map<String, StateTransferStatus>> implements BiConsumer<String, StateTransferStatus> {

   private final Map<String, StateTransferStatus> result = new HashMap<>();
   private Exception exception;

   @Override
   public Map<String, StateTransferStatus> finish() {
      if (exception != null) {
         throw CompletableFutures.asCompletionException(exception);
      }
      return result;
   }

   @Override
   protected Map<String, StateTransferStatus> addValidResponse(Address sender, ValidResponse response) {
      //noinspection unchecked
      Map<String, StateTransferStatus> rsp = (Map<String, StateTransferStatus>) response.getResponseValue();
      rsp.forEach(this);
      return null;
   }

   @Override
   protected Map<String, StateTransferStatus> addTargetNotFound(Address sender) {
      return null;
   }

   @Override
   protected Map<String, StateTransferStatus> addException(Address sender, Exception exception) {
      recordException(exception);
      return null;
   }

   private void recordException(Exception e) {
      if (this.exception == null) {
         this.exception = e;
      } else {
         this.exception.addSuppressed(e);
      }
   }

   @Override
   public void accept(String site, StateTransferStatus status) {
      result.merge(site, status, StateTransferStatus::merge);
   }
}
