package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.Response;
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
 * @since 9.1
 */
public class SingleResponseCollector extends ValidSingleResponseCollector<Response> {
   public static final SingleResponseCollector INSTANCE = new SingleResponseCollector();

   @Override
   protected Response withValidResponse(Address sender, ValidResponse response) {
      return response;
   }

   @Override
   protected Response targetNotFound(Address sender) {
      throw ResponseCollectors.remoteNodeSuspected(sender);
   }
}
