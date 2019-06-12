package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;

import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exposes information about the cluster.
 *
 * @since 10.0
 */
public class ClusterResource implements ResourceHandler {

   private final Health health;
   private final ObjectMapper mapper;

   public ClusterResource(Health health, ObjectMapper mapper) {
      this.health = health;
      this.mapper = mapper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/cluster").handleWith(this::getInfo)
            .invocation().methods(HEAD).path("/v2/cluster").handleWith(this::headHandler)
            .create();
   }

   private RestResponse headHandler(RestRequest restRequest) {
      return new NettyRestResponse.Builder().contentType(APPLICATION_JSON).status(OK).build();
   }

   private RestResponse getInfo(RestRequest restRequest) {
      ClusterHealth clusterHealth = health.getClusterHealth();
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      try {
         byte[] bytes = mapper.writeValueAsBytes(clusterHealth);
         responseBuilder.contentType(APPLICATION_JSON)
               .entity(bytes)
               .status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return responseBuilder.build();
   }
}
