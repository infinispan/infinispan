package org.infinispan.rest;

import java.util.Optional;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 9.2
 */
public class InfinispanErrorResponse extends InfinispanResponse {

   private InfinispanErrorResponse(Optional<InfinispanRequest> request) {
      super(request);
   }

   public static InfinispanErrorResponse asError(InfinispanRequest request, HttpResponseStatus status, String description) {
      InfinispanErrorResponse infinispanResponse = new InfinispanErrorResponse(Optional.ofNullable(request));
      infinispanResponse.status(status);
      if (description != null) {
         infinispanResponse.contentAsText(description);
      }
      return infinispanResponse;
   }
}
