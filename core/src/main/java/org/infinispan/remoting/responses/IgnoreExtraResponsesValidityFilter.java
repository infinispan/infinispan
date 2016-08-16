package org.infinispan.remoting.responses;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.transport.Address;

/**
 * A filter that only expects responses from an initial set of targets.
 *
 * Useful when sending a command to {@code null} to ensure we don't wait for responses from
 * cluster members that weren't properly started when the command was sent.
 *
 * JGroups calls our handler while holding a lock, so we don't need any synchronization.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
public final class IgnoreExtraResponsesValidityFilter implements ResponseFilter {

   private final Set<Address> targets;
   private int missingResponses;

   public IgnoreExtraResponsesValidityFilter(Collection<Address> targets, Address self, boolean removeSelf) {
      this.targets = new HashSet<Address>(targets);
      this.missingResponses = targets.size();
      if (removeSelf && this.targets.contains(self)) {
         missingResponses--;
      }
   }

   @Override
   public boolean isAcceptable(Response response, Address address) {
      if (targets.contains(address)) {
         missingResponses--;
      }

      // always return true to make sure a response is logged by the JGroups RpcDispatcher.
      return true;
   }

   @Override
   public boolean needMoreResponses() {
      return missingResponses > 0;
   }

}
