package org.infinispan.rest.search;

import java.util.Optional;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.query.remote.json.JsonQueryErrorResult;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 9.2
 */
public class InfinispanSearchResponse extends InfinispanResponse {

   private InfinispanSearchResponse(InfinispanRequest request) {
      super(Optional.of(request));
      contentType(MediaType.APPLICATION_JSON_TYPE);
   }

   public static InfinispanSearchResponse inReplyTo(InfinispanSearchRequest infinispanSearchRequest) {
      return new InfinispanSearchResponse(infinispanSearchRequest);
   }

   public static InfinispanSearchResponse badRequest(InfinispanSearchRequest infinispanSearchRequest, String message, String cause) {
      InfinispanSearchResponse searchResponse = new InfinispanSearchResponse(infinispanSearchRequest);
      searchResponse.status(HttpResponseStatus.BAD_REQUEST);
      searchResponse.contentAsBytes(new JsonQueryErrorResult(message, cause).asBytes());
      return searchResponse;
   }

}
