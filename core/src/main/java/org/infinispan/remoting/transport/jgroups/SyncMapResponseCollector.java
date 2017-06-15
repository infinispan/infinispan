package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Response collector supporting {@link JGroupsTransport#invokeRemotelyAsync(Collection, ReplicableCommand, ResponseMode, long, ResponseFilter, DeliverOrder, boolean)}.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class SyncMapResponseCollector extends ValidResponseCollector<Map<Address, Response>> {
   private final HashMap<Address, Response> map;
   private final boolean ignoreLeavers;
   private Exception exception;

   public SyncMapResponseCollector(boolean ignoreLeavers, int expectedSize) {
      this.map = new HashMap<>(CollectionFactory.computeCapacity(expectedSize));
      this.ignoreLeavers = ignoreLeavers;
   }

   @Override
   protected Map<Address, Response> addTargetNotFound(Address sender) {
      if (ignoreLeavers) {
         map.put(sender, CacheNotFoundResponse.INSTANCE);
         return null;
      } else {
         throw ResponseCollectors.remoteNodeSuspected(sender);
      }
   }

   @Override
   protected Map<Address, Response> addException(Address sender, Exception exception) {
      Exception e = ResponseCollectors.wrapRemoteException(sender, exception);

      if (this.exception == null) {
         this.exception = e;
      } else {
         this.exception.addSuppressed(e);
      }
      return null;
   }

   @Override
   protected Map<Address, Response> addValidResponse(Address sender, ValidResponse response) {
      map.put(sender, response);
      return null;
   }

   @Override
   public Map<Address, Response> finish() {
      if (exception != null) {
         throw CompletableFutures.asCompletionException(exception);
      }
      return map;
   }
}
