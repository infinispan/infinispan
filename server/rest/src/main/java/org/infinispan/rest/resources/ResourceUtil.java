package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.RestResponse;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Util class for REST resources.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
class ResourceUtil {
   static CompletableFuture<RestResponse> notFoundResponseFuture() {
      return CompletableFuture.completedFuture(
            new NettyRestResponse.Builder()
                  .status(HttpResponseStatus.NOT_FOUND)
                  .build()
      );
   }

   static NettyRestResponse.Builder addEntityAsJson(Json json, NettyRestResponse.Builder responseBuilder) {
      responseBuilder.contentType(APPLICATION_JSON);
      return responseBuilder.entity(json.toString()).status(OK);
   }

   static RestResponse asJsonResponse(Json json) {
      return addEntityAsJson(json, new NettyRestResponse.Builder()).build();
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Json json) {
      return completedFuture(asJsonResponse(json));
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Json json, NettyRestResponse.Builder responseBuilder) {
      RestResponse response = addEntityAsJson(json, responseBuilder).build();
      return completedFuture(response);
   }

   static NettyRestResponse.Builder addEntityAsJson(JsonSerialization o, NettyRestResponse.Builder responseBuilder) {
      responseBuilder.contentType(APPLICATION_JSON);
      return responseBuilder.entity(o.toJson().toString()).status(OK);
   }

}
