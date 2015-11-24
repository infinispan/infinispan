package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;

/**
 * A filter that tests the validity of {@link org.infinispan.commands.remote.ClusteredGetCommand}s.
 *
 * JGroups calls our handler while holding a lock, so we don't need any synchronization.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ClusteredGetResponseValidityFilter implements ResponseFilter {

   private Collection<Address> targets;
   private int validResponses;
   private int missingResponses;

   public ClusteredGetResponseValidityFilter(Collection<Address> targets, Address self) {
      this.targets = new HashSet<Address>(targets);
      this.validResponses = 0;
      this.missingResponses = targets.size();
      if (this.targets.contains(self)) {
         this.missingResponses--;
      }
   }

   @Override
   public boolean isAcceptable(Response response, Address address) {
      if (targets.contains(address)) {
         missingResponses--;
         if (response instanceof SuccessfulResponse) {
            validResponses++;
            return true;
         }
      }
      return false;
   }

   @Override
   public boolean needMoreResponses() {
      return validResponses < 1 && missingResponses > 0;
   }

}
