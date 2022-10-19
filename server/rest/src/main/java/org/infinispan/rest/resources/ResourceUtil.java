package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.RestRequest;
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

   static CompletionStage<RestResponse> noContentResponseFuture() {
      return responseFuture(NO_CONTENT);
   }

   static RestResponse noContent() {
      return response(NO_CONTENT);
   }

   static CompletableFuture<RestResponse> badRequestResponseFuture(Object message) {
      return responseFuture(BAD_REQUEST, message);
   }

   static NettyRestResponse.Builder addEntityAsJson(Json json, NettyRestResponse.Builder responseBuilder, boolean pretty) {
      responseBuilder.contentType(APPLICATION_JSON);
      return responseBuilder.entity(json.toString()).status(OK);
   }

   static RestResponse asJsonResponse(Json json, boolean pretty) {
      return addEntityAsJson(json, new NettyRestResponse.Builder(), pretty).build();
   }

   static RestResponse asJsonResponse(Json json, NettyRestResponse.Builder responseBuilder, boolean pretty) {
      return addEntityAsJson(json, responseBuilder, pretty).build();
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Json json, boolean pretty) {
      return completedFuture(asJsonResponse(json, pretty));
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Json json, NettyRestResponse.Builder responseBuilder, boolean pretty) {
      RestResponse response = addEntityAsJson(json, responseBuilder, pretty).build();
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
