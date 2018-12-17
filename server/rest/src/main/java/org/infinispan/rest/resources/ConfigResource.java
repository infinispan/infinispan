package org.infinispan.rest.resources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST resource to manage cache configurations.
 *
 * @since 10.0
 */
public class ConfigResource implements ResourceHandler {

   private static final ParserRegistry PARSER_REGISTRY = new ParserRegistry();
   private static final JsonWriter JSON_WRITER = new JsonWriter();
   private final EmbeddedCacheManager cacheManager;

   public ConfigResource(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/configurations/{name}").handleWith(this::getConfiguration)
            .invocation().methods(POST).path("/v2/configurations").withAction("toJSON").handleWith(this::convertToJson)
            .create();
   }

   private RestResponse getConfiguration(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String configurationName = restRequest.variables().get("name");

      MediaType accept = getAccept(restRequest);
      responseBuilder.contentType(accept);

      Configuration cacheConfiguration = cacheManager.getCacheConfiguration(configurationName);

      if (cacheConfiguration == null) return responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).build();

      String entity;
      if (accept.getTypeSubtype().equals(APPLICATION_XML_TYPE)) {
         entity = cacheConfiguration.toXMLString();
      } else {
         entity = JSON_WRITER.toJSON(cacheConfiguration);
      }
      return responseBuilder.entity(entity).build();
   }

   private MediaType getAccept(RestRequest restRequest) {
      String acceptHeader = restRequest.getAcceptHeader();
      if (acceptHeader == null || acceptHeader.equals(MediaType.MATCH_ALL_TYPE)) return MediaType.APPLICATION_JSON;

      MediaType accept = MediaType.parse(acceptHeader);
      if (!MediaType.APPLICATION_XML.match(accept) && !MediaType.APPLICATION_JSON.match(accept)) {
         throw new UnacceptableDataFormatException("Only json and xml are supported");
      }
      return accept;
   }

   private RestResponse convertToJson(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      ContentSource contents = restRequest.contents();

      ConfigurationBuilderHolder builderHolder = PARSER_REGISTRY.parse(new String(contents.rawContent(), UTF_8));
      ConfigurationBuilder builder = builderHolder.getNamedConfigurationBuilders().values().iterator().next();
      Configuration configuration = builder.build();

      return responseBuilder
            .contentType(APPLICATION_JSON)
            .entity(JSON_WRITER.toJSON(configuration))
            .build();
   }
}
