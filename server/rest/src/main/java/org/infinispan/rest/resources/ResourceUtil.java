package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
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

   static RestResponse response(HttpResponseStatus status) {
      return response(status, null);
   }

   static RestResponse response(HttpResponseStatus status, Object entity) {
      return new NettyRestResponse.Builder()
            .status(status)
            .entity(entity)
            .build();
   }

   static CompletableFuture<RestResponse> responseFuture(HttpResponseStatus status) {
      return responseFuture(status, null);
   }

   static CompletableFuture<RestResponse> responseFuture(HttpResponseStatus status, Object entity) {
      return CompletableFuture.completedFuture(response(status, entity));
   }

   static CompletableFuture<RestResponse> notFoundResponseFuture() {
      return responseFuture(NOT_FOUND);
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
