package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

/**
 * Util class for REST resources.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
class ResourceUtil {

   static NettyRestResponse.Builder addEntityAsJson(Json json, NettyRestResponse.Builder responseBuilder, boolean pretty) {
      responseBuilder.contentType(APPLICATION_JSON);
      return responseBuilder.entity(json.toString()).status(OK);
   }

   static RestResponse asJsonResponse(NettyRestResponse.Builder builder, Json json, boolean pretty) {
      return addEntityAsJson(json, builder, pretty).build();
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(NettyRestResponse.Builder builder, Json json, boolean pretty) {
      RestResponse response = addEntityAsJson(json, builder, pretty).build();
      return completedFuture(response);
   }

   static NettyRestResponse.Builder addEntityAsJson(JsonSerialization o, NettyRestResponse.Builder responseBuilder) {
      responseBuilder.contentType(APPLICATION_JSON);
      return responseBuilder.entity(o.toJson().toString()).status(OK);
   }

   static boolean isPretty(RestRequest request) {
      return Boolean.parseBoolean(request.getParameter("pretty"));
   }
}
