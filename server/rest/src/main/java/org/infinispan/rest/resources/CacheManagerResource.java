package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.stats.CacheContainerStats;

import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.Flowable;

/**
 * REST resource to manage the cache container.
 *
 * @since 10.0
 */
public class CacheManagerResource implements ResourceHandler {

   private final EmbeddedCacheManager cacheManager;
   private final InternalCacheRegistry internalCacheRegistry;
   private final JsonWriter jsonWriter = new JsonWriter();
   private final ObjectMapper objectMapper;
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final String cacheManagerName;

   public CacheManagerResource(InvocationHelper invocationHelper) {
      this.objectMapper = invocationHelper.getMapper();
      this.cacheManager = invocationHelper.getRestCacheManager().getInstance();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      this.cacheManagerName = globalConfiguration.cacheManagerName();
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Health
            .invocation().methods(GET, HEAD).path("/v2/cache-managers/{name}/health").handleWith(this::getHealth)
            .invocation().methods(GET, HEAD).anonymous(true).path("/v2/cache-managers/{name}/health/status").handleWith(this::getHealthStatus)

            // Config
            .invocation().methods(GET).path("/v2/cache-managers/{name}/cache-configs").handleWith(this::getAllCachesConfiguration)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/config").withAction("toJSON").handleWith(this::convertToJson)

            // Cache Manager config
            .invocation().methods(GET).path("/v2/cache-managers/{name}/config").handleWith(this::getConfig)

            // Cache Manager info
            .invocation().methods(GET).path("/v2/cache-managers/{name}").handleWith(this::getInfo)

            // Stats
            .invocation().methods(GET).path("/v2/cache-managers/{name}/stats").handleWith(this::getStats)

            // Caches
            .invocation().methods(GET).path("/v2/cache-managers/{name}/caches").handleWith(this::getCaches)
            .create();
   }

   private CompletionStage<RestResponse> getInfo(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      try {
         byte[] bytes = objectMapper.writeValueAsBytes(cacheManager.getCacheManagerInfo());
         responseBuilder.contentType(APPLICATION_JSON)
               .entity(bytes)
               .status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      GlobalConfiguration globalConfiguration = cacheManager.getCacheManagerConfiguration();

      MediaType format = MediaTypeUtils.negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML);

      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         parserRegistry.serialize(baos, globalConfiguration, emptyMap());
         if (format.match(APPLICATION_XML)) {
            responseBuilder.contentType(APPLICATION_XML);
            responseBuilder.entity(baos.toByteArray());
         } else {
            responseBuilder.contentType(APPLICATION_JSON);
            responseBuilder.entity(jsonWriter.toJSON(globalConfiguration));
         }
      } catch (XMLStreamException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }

      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      CacheContainerStats stats = cacheManager.getStats();

      try {
         byte[] bytes = objectMapper.writeValueAsBytes(stats);
         responseBuilder.contentType(APPLICATION_JSON)
               .entity(bytes)
               .status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }


   private CompletionStage<RestResponse> getHealth(RestRequest restRequest) {
      return getHealth(restRequest, false);
   }

   private CompletionStage<RestResponse> getHealthStatus(RestRequest restRequest) {
      return getHealth(restRequest, true);
   }

   private CompletionStage<RestResponse> getHealth(RestRequest restRequest, boolean anon) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(restRequest);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      if (restRequest.method() == HEAD) return completedFuture(new NettyRestResponse.Builder().status(OK).build());

      try {
         Health health = SecurityActions.getHealth(cacheManager);
         HealthInfo healthInfo = new HealthInfo(health.getClusterHealth(), health.getCacheHealth());

         MediaType contentType = anon ? TEXT_PLAIN : APPLICATION_JSON;
         Object payload = anon ? healthInfo.clusterHealth.getHealthStatus().toString() : objectMapper.writeValueAsBytes(healthInfo);
         responseBuilder.contentType(contentType)
               .entity(payload)
               .status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getCaches(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      // We rely on the fact that getCacheNames doesn't block for embedded - remote it does unfortunately
      return Flowable.fromIterable(cacheManager.getCacheNames())
            .map(cacheManager::getCache)
            .flatMapSingle(cache ->
                  RxJavaInterop.completionStageToSingle(cache.sizeAsync())
                        .map(size -> {
                           CacheInfo cacheInfo = new CacheInfo();
                           cacheInfo.name = cache.getName();
                           Configuration cacheConfiguration = cache.getCacheConfiguration();
                           cacheInfo.type = cacheConfiguration.clustering().cacheMode().toCacheType();
                           cacheInfo.status = cache.getStatus().name();
                           cacheInfo.size = size;
                           cacheInfo.simpleCache = cacheConfiguration.simpleCache();
                           cacheInfo.transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
                           cacheInfo.persistent = cacheConfiguration.persistence().usingStores();
                           cacheInfo.bounded = cacheConfiguration.expiration().maxIdle() != -1 ||
                                 cacheConfiguration.expiration().lifespan() != -1;
                           cacheInfo.secured = cacheConfiguration.security().authorization().enabled();
                           cacheInfo.indexed = cacheConfiguration.indexing().index().isEnabled();
                           cacheInfo.hasRemoteBackup = cacheConfiguration.sites().hasEnabledBackups();
                           return cacheInfo;
                           // Only request 1 cache size at a time
                        }), false, 1)
            .collectInto(new HashSet<>(), Set::add)
            .map(caches -> {
               try {
                  byte[] bytes = objectMapper.writeValueAsBytes(caches);
                  responseBuilder.contentType(APPLICATION_JSON).entity(bytes);
               } catch (JsonProcessingException e) {
                  responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
               }
               return responseBuilder.build();
            })
            .to(RxJavaInterop.singleToCompletionStage());
   }

   private CompletionStage<RestResponse> getAllCachesConfiguration(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      try {
         Set<String> cacheConfigurationNames = cacheManager.getCacheConfigurationNames();

         Set<NamedCacheConfiguration> configurations = cacheConfigurationNames.stream()
               .filter(n -> !internalCacheRegistry.isInternalCache(n))
               .map(n -> {
                  Configuration cacheConfiguration = cacheManager.getCacheConfiguration(n);
                  String json = jsonWriter.toJSON(cacheConfiguration);
                  return new NamedCacheConfiguration(n, json);
               }).collect(Collectors.toSet());

         byte[] bytes = objectMapper.writeValueAsBytes(configurations);
         responseBuilder.contentType(APPLICATION_JSON).entity(bytes);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }

      return completedFuture(responseBuilder.build());
   }


   private CompletionStage<RestResponse> convertToJson(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(restRequest);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      ContentSource contents = restRequest.contents();

      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(new String(contents.rawContent(), UTF_8));
      ConfigurationBuilder builder = builderHolder.getNamedConfigurationBuilders().values().iterator().next();
      Configuration configuration = builder.build();
      responseBuilder.contentType(APPLICATION_JSON)
            .entity(jsonWriter.toJSON(configuration));
      return completedFuture(responseBuilder.build());
   }

   private NettyRestResponse.Builder checkCacheManager(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      String name = request.variables().get("name");
      if (!name.equals(cacheManagerName)) {
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);
      }
      return responseBuilder;

   }

   class HealthInfo {
      private final ClusterHealth clusterHealth;
      private final List<CacheHealth> cacheHealth;

      HealthInfo(ClusterHealth clusterHealth, List<CacheHealth> cacheHealth) {
         this.clusterHealth = clusterHealth;
         this.cacheHealth = cacheHealth;
      }

      public ClusterHealth getClusterHealth() {
         return clusterHealth;
      }

      public List<CacheHealth> getCacheHealth() {
         return cacheHealth;
      }
   }

   class NamedCacheConfiguration {
      String name;

      Object configuration;

      NamedCacheConfiguration(String name, Object configuration) {
         this.name = name;
         this.configuration = configuration;
      }

      public String getName() {
         return name;
      }

      @JsonRawValue
      public Object getConfiguration() {
         return configuration;
      }
   }

   class CacheInfo {
      public String status;
      public String name;
      public String type;
      public long size;
      public boolean simpleCache;
      public boolean transactional;
      public boolean persistent;
      public boolean bounded;
      public boolean indexed;
      public boolean secured;
      public boolean hasRemoteBackup;
   }

}
