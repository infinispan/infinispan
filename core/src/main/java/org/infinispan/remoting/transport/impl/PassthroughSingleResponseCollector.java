package org.infinispan.remoting.transport.impl;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;

/**
 * RPC to a single node, without any validity checks.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class PassthroughSingleResponseCollector implements ResponseCollector<Response> {
   public static final PassthroughSingleResponseCollector INSTANCE = new PassthroughSingleResponseCollector();

   // No need for new instances
   private PassthroughSingleResponseCollector() {}

   @Override
   public Response addResponse(Address sender, Response response) {
      return response;
   }

   @Override
   public Response finish() {
      return null;
   }
}
