package org.infinispan.interceptors.distribution;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.statetransfer.OutdatedTopologyException;

/**
 * Return the first successful response for a staggered remote get, used in dist mode.
 *
 * Throw an {@link OutdatedTopologyException} if all responses are either {@link UnsureResponse} or
 * {@link CacheNotFoundResponse}.
 * Throw an exception immediately if a response is exceptional or unexpected.
 *
 * @author Dan Berindei
 * @since 9.4
 */
public class RemoteGetSingleKeyCollector implements ResponseCollector<SuccessfulResponse> {
   private boolean hasSuspectResponse;
   private boolean hasUnsureResponse;

   @Override
   public SuccessfulResponse addResponse(Address sender, Response response) {
      if (response.isSuccessful()) {
         return (SuccessfulResponse) response;
      }
      if (response instanceof ExceptionResponse) {
         throw ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException());
      }

      if (response instanceof UnsureResponse) {
         hasUnsureResponse = true;
         return null;
      } else if (response instanceof CacheNotFoundResponse) {
         hasSuspectResponse = true;
         return null;
      }
      throw ResponseCollectors.unexpectedResponse(response);
   }

   @Override
   public SuccessfulResponse finish() {
      // We got UnsureResponse or CacheNotFoundResponse from all the targets: all of them either have a newer
      // topology or are no longer in the cluster.
      if (hasSuspectResponse) {
         // We got at least one CacheNotFoundResponses, but we don't give up because write owners might have a copy.
         // Wait for a new topology to avoid an infinite loop.
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      } else {
         // Safe to retry in the same topology without an infinite loop, see the javadoc.
         throw OutdatedTopologyException.RETRY_SAME_TOPOLOGY;
      }
   }
}
