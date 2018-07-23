package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;

/**
 * Response collector for a single response.
 *
 * Returns {@link CacheNotFoundResponse#INSTANCE} if the node leaves or has already left the cluster.
 *
 * This is implemented as separate class from {@link SingleResponseCollector} since it does not return {@link ValidResponse}.
 */
public class SingleResponseIgnoreLeaversCollector extends ValidSingleResponseCollector<Response> {
   private static final SingleResponseIgnoreLeaversCollector INSTANCE = new SingleResponseIgnoreLeaversCollector();

   public static SingleResponseIgnoreLeaversCollector instance() {
      return INSTANCE;
   }

   @Override
   protected ValidResponse withValidResponse(Address sender, ValidResponse response) {
      return response;
   }

   @Override
   protected Response targetNotFound(Address sender) {
      return CacheNotFoundResponse.INSTANCE;
   }
}
