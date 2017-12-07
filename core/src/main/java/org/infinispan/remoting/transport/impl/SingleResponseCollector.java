package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;

/**
 * Response collector for a single response.
 *
 * Throws a {@link org.infinispan.remoting.transport.jgroups.SuspectException} if the node leaves
 * or has already left the cluster.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class SingleResponseCollector extends ValidSingleResponseCollector<ValidResponse> {
   private static final SingleResponseCollector VALID_ONLY = new SingleResponseCollector();

   public static SingleResponseCollector validOnly() {
      return VALID_ONLY;
   }

   @Override
   protected ValidResponse withValidResponse(Address sender, ValidResponse response) {
      return response;
   }

   @Override
   protected ValidResponse targetNotFound(Address sender) {
      throw ResponseCollectors.remoteNodeSuspected(sender);
   }
}
