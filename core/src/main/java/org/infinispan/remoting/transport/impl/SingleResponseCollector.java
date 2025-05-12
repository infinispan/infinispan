package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.ValidResponse;
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
public class SingleResponseCollector<S> extends ValidSingleResponseCollector<S, ValidResponse> {
   private static final SingleResponseCollector VALID_ONLY = new SingleResponseCollector<>();

   public static <S> SingleResponseCollector<S> validOnly() {
      return VALID_ONLY;
   }

   @Override
   protected ValidResponse withValidResponse(S sender, ValidResponse response) {
      return response;
   }

   @Override
   protected ValidResponse targetNotFound(S sender) {
      throw ResponseCollectors.remoteNodeSuspected(sender);
   }
}
