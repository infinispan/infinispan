package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.concurrent.CompletableFuture;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Util class for REST resources.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
class ResourceUtil {
   private final static Log logger = LogFactory.getLog(ResourceUtil.class, Log.class);

   static CompletableFuture<RestResponse> notFoundResponseFuture() {
      return CompletableFuture.completedFuture(
            new NettyRestResponse.Builder()
                  .status(HttpResponseStatus.NOT_FOUND)
                  .build()
      );
   }

   static RestResponse asJsonResponse(Object o, InvocationHelper invocationHelper) {
      return addEntityAsJson(o, new NettyRestResponse.Builder(), invocationHelper).build();
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Object o, InvocationHelper invocationHelper) {
      return completedFuture(asJsonResponse(o, invocationHelper));
   }

   static CompletableFuture<RestResponse> asJsonResponseFuture(Object o, NettyRestResponse.Builder responseBuilder, InvocationHelper invocationHelper) {
      RestResponse response = addEntityAsJson(o, responseBuilder, invocationHelper).build();
      return completedFuture(response);
   }

   static NettyRestResponse.Builder addEntityAsJson(Object o, NettyRestResponse.Builder responseBuilder, InvocationHelper invocationHelper) {
      responseBuilder.contentType(APPLICATION_JSON);
      ObjectMapper mapper = invocationHelper.getMapper();
      try {
         byte[] bytes = mapper.writeValueAsBytes(o);
         return responseBuilder.entity(bytes).status(OK);
      } catch (JsonProcessingException e) {
         logger.error(e);
         Object entity = createJsonErrorResponse(mapper, e);
         return responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(entity);
      }
   }

   private static Object createJsonErrorResponse(ObjectMapper mapper, JsonProcessingException e) {
      try {
         return mapper.writeValueAsBytes(new JsonErrorResponseEntity("Unable to convert response object to json", e.getMessage()));
      } catch (JsonProcessingException jpe) {
         logger.error(jpe);
         return jpe.getMessage();
      }
   }
}
