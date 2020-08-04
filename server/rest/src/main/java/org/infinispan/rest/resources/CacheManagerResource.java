package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.io.ByteArrayOutputStream;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.Security;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.CacheIgnoreManager;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Flowable;

/**
 * REST resource to manage the cache container.
 *
 * @since 10.0
 */
public class CacheManagerResource implements ResourceHandler {

   private final InvocationHelper invocationHelper;
   private final EmbeddedCacheManager cacheManager;
   private final InternalCacheRegistry internalCacheRegistry;
   private final JsonWriter jsonWriter = new JsonWriter();
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final String cacheManagerName;
   private final RestCacheManager<Object> restCacheManager;
   private final CacheIgnoreManager cacheIgnoreManager;

   public CacheManagerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.cacheManager = invocationHelper.getRestCacheManager().getInstance();
      this.restCacheManager = invocationHelper.getRestCacheManager();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      this.cacheManagerName = globalConfiguration.cacheManagerName();
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      this.cacheIgnoreManager = globalComponentRegistry.getComponent(CacheIgnoreManager.class);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Health
            .invocation().methods(GET, HEAD).path("/v2/cache-managers/{name}/health").handleWith(this::getHealth)
            .invocation().methods(GET, HEAD).anonymous(true).path("/v2/cache-managers/{name}/health/status").handleWith(this::getHealthStatus)

            // Config
            .invocation().methods(GET).path("/v2/cache-managers/{name}/cache-configs").handleWith(this::getAllCachesConfiguration)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/cache-configs/templates").handleWith(this::getAllCachesConfigurationTemplates)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/config").withAction("toJSON").handleWith(this::convertToJson)

            // Cache Manager config
            .invocation().methods(GET).path("/v2/cache-managers/{name}/config").handleWith(this::getConfig)

            // Cache Manager info
            .invocation().methods(GET).path("/v2/cache-managers/{name}").handleWith(this::getInfo)

            // Stats
            .invocation().methods(GET).path("/v2/cache-managers/{name}/stats").handleWith(this::getStats)

            // Caches
            .invocation().methods(GET).path("/v2/cache-managers/{name}/caches").handleWith(this::getCaches)

            // BackupManager
            .invocation().methods(GET).path("/v2/cache-managers/{name}/backups").handleWith(this::getAllBackupNames)
            .invocation().methods(DELETE, GET, HEAD, POST).path("/v2/cache-managers/{name}/backups/{backupName}").handleWith(this::backup)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/backups").withAction("restore").handleWith(this::restore)
            .create();
   }

   private CompletionStage<RestResponse> getInfo(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      return asJsonResponseFuture(cacheManager.getCacheManagerInfo().toJson(), responseBuilder);
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager embeddedCacheManager = cacheManager.withSubject(request.getSubject());

      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(embeddedCacheManager);

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

      return asJsonResponseFuture(cacheManager.getStats().toJson(), responseBuilder);
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

      Health health = cacheManager.withSubject(restRequest.getSubject()).getHealth();
      HealthInfo healthInfo = new HealthInfo(health.getClusterHealth(), health.getCacheHealth());

      if (anon) {
         responseBuilder
               .contentType(TEXT_PLAIN)
               .entity(Security.doAs(restRequest.getSubject(), (PrivilegedAction<String>)
                     () -> healthInfo.clusterHealth.getHealthStatus().toString()))
               .status(OK);
      } else {
         addEntityAsJson(healthInfo.toJson(), responseBuilder);
      }
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getCaches(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());
      Map<String, HealthStatus> cachesHealth = new HashMap<>();
      // Remove internal caches
      Set<String> cacheNames = new HashSet<>(subjectCacheManager.getCacheNames());
      cacheNames.removeAll(internalCacheRegistry.getInternalCacheNames());


      Set<String> ignoredCaches = cacheIgnoreManager.getIgnoredCaches();
      for (CacheHealth ch : SecurityActions.getHealth(subjectCacheManager).getCacheHealth(cacheNames)) {
         cachesHealth.put(ch.getCacheName(), ch.getStatus());
      }

      // We rely on the fact that getCacheNames doesn't block for embedded - remote it does unfortunately
      return Flowable.fromIterable(cachesHealth.entrySet())
            .map(chHealth -> {
               CacheInfo cacheInfo = new CacheInfo();
               String cacheName = chHealth.getKey();
               cacheInfo.name = cacheName;
               cacheInfo.health = cachesHealth.get(cacheName);
               Configuration cacheConfiguration = SecurityActions
                     .getCacheConfigurationFromManager(subjectCacheManager, cacheName);
               cacheInfo.type = cacheConfiguration.clustering().cacheMode().toCacheType();

               cacheInfo.simpleCache = cacheConfiguration.simpleCache();
               cacheInfo.transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
               cacheInfo.persistent = cacheConfiguration.persistence().usingStores();
               cacheInfo.bounded = cacheConfiguration.expiration().maxIdle() != -1 ||
                     cacheConfiguration.expiration().lifespan() != -1;
               cacheInfo.secured = cacheConfiguration.security().authorization().enabled();
               cacheInfo.indexed = cacheConfiguration.indexing().enabled();
               cacheInfo.hasRemoteBackup = cacheConfiguration.sites().hasEnabledBackups();

               // If the cache is ignored, status is IGNORED
               if (ignoredCaches.contains(cacheName)) {
                  cacheInfo.status = "IGNORED";
               } else {
                  // This action will fail for ignored caches
                  Cache cache = restCacheManager.getCache(cacheName, request);
                  cacheInfo.status = cache.getStatus().toString();
               }
               return cacheInfo;
            })
            .collectInto(new HashSet<CacheInfo>(), Set::add)
            .map(cacheInfos -> {
               List<CacheInfo> sortedCacheInfos = cacheInfos.stream()
                     .sorted(Comparator.comparing(c -> c.name))
                     .collect(Collectors.toList());
               return (RestResponse) addEntityAsJson(Json.make(sortedCacheInfos), responseBuilder).build();
            })
            .toCompletionStage();
   }

   private CompletionStage<RestResponse> getAllCachesConfiguration(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());

      Set<String> cacheConfigurationNames = subjectCacheManager.getCacheConfigurationNames();

      List<NamedCacheConfiguration> configurations = cacheConfigurationNames.stream()
            .filter(n -> !internalCacheRegistry.isInternalCache(n))
            .distinct()
            .map(n -> {
               Configuration cacheConfiguration = SecurityActions
                     .getCacheConfigurationFromManager(subjectCacheManager, n);
               String json = jsonWriter.toJSON(cacheConfiguration);
               return new NamedCacheConfiguration(n, json);
            })
            .sorted(Comparator.comparing(c -> c.name))
            .collect(Collectors.toList());

      return asJsonResponseFuture(Json.make(configurations), responseBuilder);
   }

   private CompletionStage<RestResponse> getAllCachesConfigurationTemplates(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = checkCacheManager(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());

      Set<String> cacheConfigurationNames = subjectCacheManager.getCacheConfigurationNames();

      List<NamedCacheConfiguration> configurations = cacheConfigurationNames.stream()
            .filter(n -> !internalCacheRegistry.isInternalCache(n))
            .filter(n -> SecurityActions.getCacheConfigurationFromManager(subjectCacheManager, n).isTemplate())
            .distinct()
            .map(n -> {
               Configuration config = SecurityActions.getCacheConfigurationFromManager(subjectCacheManager, n);
               return new NamedCacheConfiguration(n, jsonWriter.toJSON(config));
            })
            .sorted(Comparator.comparing(c -> c.name))
            .collect(Collectors.toList());

      return asJsonResponseFuture(Json.make(configurations), responseBuilder);
   }

   private CompletionStage<RestResponse> getAllBackupNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = backupManager.getBackupNames();
      return asJsonResponseFuture(Json.make(names));
   }

   private CompletionStage<RestResponse> backup(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      return BackupManagerResource.handleBackupRequest(request, backupManager, (name, workingDir, json) -> {
         BackupManager.Resources resources = BackupManagerResource.getResources(json);
         Map<String, BackupManager.Resources> backupParams = Collections.singletonMap(cacheManagerName, resources);
         backupManager.create(name, workingDir, backupParams);
      });
   }

   private CompletionStage<RestResponse> restore(RestRequest request) {
      return BackupManagerResource.handleRestoreRequest(request, (path, json) -> {
         BackupManager.Resources resources = BackupManagerResource.getResources(json);
         Map<String, BackupManager.Resources> restoreParams = Collections.singletonMap(cacheManagerName, resources);
         BackupManager backupManager = invocationHelper.getServer().getBackupManager();
         return backupManager.restore(path, restoreParams);
      });
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

   static class HealthInfo implements JsonSerialization {
      private final ClusterHealth clusterHealth;
      private final List<CacheHealth> cacheHealth;

      HealthInfo(ClusterHealth clusterHealth, List<CacheHealth> cacheHealth) {
         this.clusterHealth = clusterHealth;
         this.cacheHealth = cacheHealth;
      }

      @Override
      public Json toJson() {
         return Json.object()
               .set("cluster_health", clusterHealth.toJson())
               .set("cache_health", Json.make(cacheHealth));
      }
   }

   static class NamedCacheConfiguration implements JsonSerialization {
      String name;
      String configuration;

      NamedCacheConfiguration(String name, String configuration) {
         this.name = name;
         this.configuration = configuration;
      }

      @Override
      public Json toJson() {
         return Json.object()
               .set("name", name)
               .set("configuration", Json.factory().raw(configuration));
      }
   }

   static class CacheInfo implements JsonSerialization {
      public String status;
      public String name;
      public String type;
      public boolean simpleCache;
      public boolean transactional;
      public boolean persistent;
      public boolean bounded;
      public boolean indexed;
      public boolean secured;
      public boolean hasRemoteBackup;
      public HealthStatus health;

      @Override
      public Json toJson() {
         return Json.object()
               .set("status", status)
               .set("name", name)
               .set("type", type)
               .set("simple_cache", simpleCache)
               .set("transactional", transactional)
               .set("persistent", persistent)
               .set("bounded", bounded)
               .set("secured", secured)
               .set("indexed", indexed)
               .set("has_remote_backup", hasRemoteBackup)
               .set("health", health);
      }
   }

}
