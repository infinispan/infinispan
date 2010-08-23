package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;

/**
 * A filter that tests the validity of {@link org.infinispan.commands.remote.ClusteredGetCommand}s.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ClusteredGetResponseValidityFilter implements ResponseFilter {

   private int numValidResponses = 0;

   private Collection<Address> pendingResponders;

   public ClusteredGetResponseValidityFilter(Collection<Address> pendingResponders) {
      this.pendingResponders = new HashSet<Address>(pendingResponders);
   }

   public boolean isAcceptable(Response response, Address address) {
      pendingResponders.remove(address);

      if (response instanceof SuccessfulResponse) numValidResponses++;

      // always return true to make sure a response is logged by the JGroups RpcDispatcher.
      return true;
   }

   public boolean needMoreResponses() {
      return numValidResponses < 1 && !pendingResponders.isEmpty();
   }

}
