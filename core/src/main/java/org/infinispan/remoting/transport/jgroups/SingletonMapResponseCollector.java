package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Response collector supporting {@link JGroupsTransport#invokeRemotelyAsync(Collection, ReplicableCommand, ResponseMode, long, ResponseFilter, DeliverOrder, boolean)}.
 *
 * @author Dan Berindei
 * @since 9.1
 */
class SingletonMapResponseCollector
      extends ValidSingleResponseCollector<Map<Address, Response>> {
   private static final Log log = LogFactory.getLog(SingletonMapResponseCollector.class);

   private final boolean ignoreLeavers;

   SingletonMapResponseCollector(boolean ignoreLeavers) {
      this.ignoreLeavers = ignoreLeavers;
   }

   @Override
   protected Map<Address, Response> withValidResponse(Address sender, ValidResponse response) {
      return Collections.singletonMap(sender, response);
   }

   @Override
   protected Map<Address, Response> targetNotFound(Address sender) {
      if (!ignoreLeavers) {
         throw log.remoteNodeSuspected(sender);
      }
      return Collections.singletonMap(sender, CacheNotFoundResponse.INSTANCE);
   }
}
