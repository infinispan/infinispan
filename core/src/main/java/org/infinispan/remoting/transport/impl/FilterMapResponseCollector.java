package org.infinispan.remoting.transport.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

/**
 * Response collector supporting {@link JGroupsTransport#invokeRemotelyAsync(Collection, ReplicableCommand, ResponseMode, long, ResponseFilter, DeliverOrder, boolean)}.
 *
 * <p>This class is not thread-safe by itself. It expects an {@link org.infinispan.remoting.transport.AbstractRequest}
 * to handle synchronization.</p>
 */
public class FilterMapResponseCollector extends ValidResponseCollector<Map<Address, Response>> {
   private final HashMap<Address, Response> map;
   private final ResponseFilter filter;
   private final boolean waitForAll;

   public FilterMapResponseCollector(ResponseFilter filter, boolean waitForAll, int expectedSize) {
      this.map = new HashMap<>(expectedSize);
      this.filter = filter;
      this.waitForAll = waitForAll;
   }

   @Override
   protected Map<Address, Response> addValidResponse(Address sender, ValidResponse response) {
      boolean isDone;
      if (filter != null) {
         boolean isAcceptable = filter.isAcceptable(response, sender);
         // We don't need to call needMoreResponses() with ResponseMode.WAIT_FOR_VALID_RESPONSE
         isDone = waitForAll ? !filter.needMoreResponses() : isAcceptable;
      } else {
         isDone = !waitForAll;
      }
      map.put(sender, response);
      return isDone ? map : null;
   }

   @Override
   protected Map<Address, Response> addTargetNotFound(Address sender) {
      // Even without a filter, we don't complete the request on invalid responses like CacheNotFoundResponse
      map.put(sender, CacheNotFoundResponse.INSTANCE);
      return null;
   }

   @Override
   protected Map<Address, Response> addException(Address sender, Exception exception) {
      throw ResponseCollectors.wrapRemoteException(sender, exception);
   }

   @Override
   public Map<Address, Response> finish() {
      return map;
   }
}
