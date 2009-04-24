package org.infinispan.remoting.responses;

import org.infinispan.remoting.ResponseFilter;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.List;

/**
 * A filter that tests the validity of {@link org.infinispan.commands.remote.ClusteredGetCommand}s.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ClusteredGetResponseValidityFilter implements ResponseFilter {

   private int numValidResponses = 0;

   private List<Address> pendingResponders;

   public ClusteredGetResponseValidityFilter(List<Address> expected, Address localAddress) {
      this.pendingResponders = new ArrayList<Address>(expected);
      // We'll never get a response from ourself
      this.pendingResponders.remove(localAddress);
   }

   public boolean isAcceptable(Response response, Address address) {
      pendingResponders.remove(address);

      if (response instanceof SuccessfulResponse) numValidResponses++;

      // always return true to make sure a response is logged by the JGroups RpcDispatcher.
      return true;
   }

   public boolean needMoreResponses() {
      return numValidResponses < 1 && pendingResponders.size() > 0;
   }

}
