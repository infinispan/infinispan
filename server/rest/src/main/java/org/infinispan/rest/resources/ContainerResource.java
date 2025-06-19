package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.ServerSentEvent;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.BackupManager;
import org.infinispan.util.logging.annotation.impl.Logged;
import org.infinispan.util.logging.events.EventLog;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogSerializer;

/**
 * REST resource to manage the cache container.
 *
 * @since 10.0
 */
public class ContainerResource implements ResourceHandler {

   private final InvocationHelper invocationHelper;
   private final InternalCacheRegistry internalCacheRegistry;
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final String cacheManagerName;

   public ContainerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      this.cacheManagerName = globalConfiguration.cacheManagerName();
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("container", "REST resource to manage the cache container.")
            // Health
            .invocation().methods(GET, HEAD).path("/v2/cache-managers/{name}/health")
            .deprecated() // TODO: remove in 16
            .handleWith(this::getHealth)
            .invocation().methods(GET, HEAD).path("/v2/container/health")
            .deprecated()
            .handleWith(this::getHealth)

            .invocation().methods(GET, HEAD).anonymous(true).path("/v2/cache-managers/{name}/health/status")
            .deprecated() // TODO: remove in 16
            .handleWith(this::getHealthStatus)
            .invocation().methods(GET, HEAD).anonymous(true).path("/v2/container/health/status")
            .handleWith(this::getHealthStatus)

            // Config
            .invocation().methods(GET).deprecated().path("/v2/cache-managers/{name}/cache-configs").handleWith(this::getAllCachesConfiguration)
            .invocation().methods(GET).deprecated().path("/v2/cache-managers/{name}/cache-configs/templates").handleWith(this::getAllCachesConfigurationTemplates)
            .invocation().methods(POST).deprecated().path("/v2/cache-managers/{name}/config").withAction("toJSON").handleWith(this::convertToJson)

            .invocation().methods(GET).path("/v2/container/cache-configs").handleWith(this::getAllCachesConfiguration)
            .invocation().methods(GET).path("/v2/container/cache-configs/templates").handleWith(this::getAllCachesConfigurationTemplates)
            .invocation().methods(POST).path("/v2/container/cache-configs/config").withAction("toJSON").handleWith(this::convertToJson)

            // Cache Manager config
            .invocation().methods(GET).deprecated().path("/v2/cache-managers/{name}/config").handleWith(this::getConfig)
            .invocation().methods(GET).path("/v2/container/config").handleWith(this::getConfig)

            // Shutdown the container content
            .invocation().methods(POST).path("/v2/container").withAction("shutdown").name("SHUTDOWN CONTAINER")
               .auditContext(AuditContext.CACHEMANAGER).handleWith(this::shutdown)

            // Container configuration listener
            .invocation().methods(GET).path("/v2/container/config").withAction("listen")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::listenConfig)

            // Container lifecycle listener
            .invocation().method(GET).path("/v2/container").withAction("listen")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).handleWith(this::listenLifecycle)

            // Cache Manager info
            .invocation().methods(GET).path("/v2/cache-managers/{name}").deprecated().handleWith(this::getInfo)
            .invocation().methods(GET).path("/v2/container").handleWith(this::getInfo)

            // Enable Rebalance
            .invocation().methods(POST).path("/v2/cache-managers/{name}").withAction("enable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("ENABLE REBALANCE GLOBAL").auditContext(AuditContext.CACHEMANAGER)
            .deprecated()
            .handleWith(r -> setRebalancing(true, r))

            .invocation().methods(POST).path("/v2/container").withAction("enable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("ENABLE REBALANCE GLOBAL").auditContext(AuditContext.CACHEMANAGER)
            .handleWith(r -> setRebalancing(true, r))

            // Disable Rebalance
            .invocation().methods(POST).path("/v2/cache-managers/{name}").withAction("disable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("DISABLE REBALANCE GLOBAL").auditContext(AuditContext.CACHEMANAGER)
            .deprecated()
            .handleWith(r -> setRebalancing(false, r))

            .invocation().methods(POST).path("/v2/container").withAction("disable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("DISABLE REBALANCE GLOBAL").auditContext(AuditContext.CACHEMANAGER)
            .handleWith(r -> setRebalancing(false, r))

            // Stats
            .invocation().methods(GET).path("/v2/cache-managers/{name}/stats").deprecated().handleWith(this::getStats)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/stats").deprecated().withAction("reset").permission(AuthorizationPermission.ADMIN).handleWith(this::resetStats)

            .invocation().methods(GET).path("/v2/container/stats").handleWith(this::getStats)
            .invocation().methods(POST).path("/v2/container/stats").withAction("reset").permission(AuthorizationPermission.ADMIN).handleWith(this::resetStats)

            // BackupManager
            .invocation().methods(GET).path("/v2/cache-managers/{name}/backups")
               .deprecated().handleWith(this::getAllBackupNames)
            .invocation().methods(DELETE, GET, HEAD, POST).path("/v2/cache-managers/{name}/backups/{backupName}")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .deprecated().handleWith(this::backup)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/restores")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .deprecated().handleWith(this::getAllRestoreNames)
            .invocation().methods(DELETE, HEAD, POST).path("/v2/cache-managers/{name}/restores/{restoreName}")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .deprecated().handleWith(this::restore)

            .invocation().methods(GET).path("/v2/container/backups").handleWith(this::getAllBackupNames)
            .invocation().methods(DELETE, GET, HEAD, POST).path("/v2/container/backups/{backupName}")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .handleWith(this::backup)
            .invocation().methods(GET).path("/v2/container/restores")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .handleWith(this::getAllRestoreNames)
            .invocation().methods(DELETE, HEAD, POST).path("/v2/container/restores/{restoreName}")
               .permission(AuthorizationPermission.ADMIN).auditContext(AuditContext.SERVER).name("BACKUP")
               .handleWith(this::restore)

            .create();
   }

   private CompletionStage<RestResponse> getInfo(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      return CompletableFuture.supplyAsync(() -> {
         // toJson is blocking as it invokes the command on the coordinator
         Json cacheManagerInfo = cacheManager.getCacheManagerInfo().toJson();
         return addEntityAsJson(cacheManagerInfo, responseBuilder, isPretty(request)).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> setRebalancing(boolean enable, RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND)
         return completedFuture(responseBuilder.build());

      return CompletableFuture.supplyAsync(()-> {
         try {
            EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
            SecurityActions.getGlobalComponentRegistry(cacheManager).getLocalTopologyManager().setRebalancingEnabled(enable);
            responseBuilder.status(NO_CONTENT);
         } catch (Exception e) {
            throw Util.unchecked(e);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager embeddedCacheManager = invocationHelper.getRestCacheManager().getInstance().withSubject(request.getSubject());

      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(embeddedCacheManager);

      MediaType format = MediaTypeUtils.negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML);

      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         try (ConfigurationWriter writer = ConfigurationWriter.to(baos).withType(format).prettyPrint(isPretty(request)).build()) {
            parserRegistry.serialize(writer, globalConfiguration, emptyMap());
         }
         responseBuilder.contentType(format);
         responseBuilder.entity(baos.toByteArray());
      } catch (Exception e) {
         throw Util.unchecked(e);
      }

      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      return CompletableFuture.supplyAsync(() -> Security.doAs(request.getSubject(), cacheManager::getStats).toJson(), invocationHelper.getExecutor())
            .thenCompose(json -> asJsonResponseFuture(invocationHelper.newResponse(request), json, isPretty(request)));
   }

   private CompletionStage<RestResponse> resetStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      return CompletableFuture.supplyAsync(() -> {
         Security.doAs(request.getSubject(), () -> cacheManager.getStats().reset());
         return invocationHelper.newResponse(request, NO_CONTENT);
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getHealth(RestRequest request) {
      return getHealth(request, false);
   }

   private CompletionStage<RestResponse> getHealthStatus(RestRequest request) {
      return getHealth(request, true);
   }

   private CompletionStage<RestResponse> getHealth(RestRequest request, boolean anon) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      // If /v2/containers?action=shutdown has been executed, we must still return OK so that the k8s probes don't fail
      // before the StatefulSet has been scaled down
      boolean isStopping = anon && invocationHelper.getServer().getStatus().isStopping();
      if (request.method() == HEAD || isStopping)
         return completedFuture(invocationHelper.newResponse(request).status(OK).build());

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      Health health = cacheManager.withSubject(request.getSubject()).getHealth();
      HealthInfo healthInfo = new HealthInfo(health.getClusterHealth(), health.getCacheHealth());

      if (anon) {
         responseBuilder
               .contentType(TEXT_PLAIN)
               .entity(Security.doAs(request.getSubject(), () -> healthInfo.clusterHealth.getHealthStatus().toString()))
               .status(OK);
      } else {
         addEntityAsJson(healthInfo.toJson(), responseBuilder, isPretty(request));
      }
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getAllCachesConfiguration(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      boolean pretty = isPretty(request);
      EmbeddedCacheManager subjectCacheManager = invocationHelper.getRestCacheManager().getInstance().withSubject(request.getSubject());

      Set<String> cacheConfigurationNames = subjectCacheManager.getCacheConfigurationNames();

      List<NamedCacheConfiguration> configurations = cacheConfigurationNames.stream()
            .filter(n -> !internalCacheRegistry.isInternalCache(n))
            .distinct()
            .map(n -> getNamedCacheConfiguration(subjectCacheManager, n, pretty))
            .sorted(Comparator.comparing(c -> c.name))
            .collect(Collectors.toList());

      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(configurations), pretty);
   }

   private CompletionStage<RestResponse> getAllCachesConfigurationTemplates(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      boolean pretty = isPretty(request);
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());

      Set<String> cacheConfigurationNames = subjectCacheManager.getCacheConfigurationNames();

      List<NamedCacheConfiguration> configurations = cacheConfigurationNames.stream()
            .filter(n -> !internalCacheRegistry.isInternalCache(n))
            .filter(n -> SecurityActions.getCacheConfiguration(subjectCacheManager, n).isTemplate())
            .distinct()
            .map(n -> {
               return getNamedCacheConfiguration(subjectCacheManager, n, pretty);
            })
            .sorted(Comparator.comparing(c -> c.name))
            .collect(Collectors.toList());

      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(configurations), pretty);
   }

   private NamedCacheConfiguration getNamedCacheConfiguration(EmbeddedCacheManager subjectCacheManager, String n, boolean pretty) {
      Configuration config = SecurityActions.getCacheConfiguration(subjectCacheManager, n);
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(pretty).build()) {
         invocationHelper.getParserRegistry().serialize(w, n, config);
      }
      return new NamedCacheConfiguration(n, sw.toString());
   }

   private CompletionStage<RestResponse> getAllBackupNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = backupManager.getBackupNames();
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(names), isPretty(request));
   }

   private CompletionStage<RestResponse> backup(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      return BackupManagerResource.handleBackupRequest(invocationHelper, request, backupManager, (name, workingDir, json) -> {
         BackupManager.Resources resources = BackupManagerResource.getResources(json);
         Map<String, BackupManager.Resources> backupParams = Collections.singletonMap(cacheManagerName, resources);
         backupManager.create(name, workingDir, backupParams);
      });
   }

   private CompletionStage<RestResponse> getAllRestoreNames(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      Set<String> names = backupManager.getRestoreNames();
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(names), isPretty(request));
   }

   private CompletionStage<RestResponse> restore(RestRequest request) {
      BackupManager backupManager = invocationHelper.getServer().getBackupManager();
      return BackupManagerResource.handleRestoreRequest(invocationHelper, request, backupManager, (name, path, json) -> {
         BackupManager.Resources resources = BackupManagerResource.getResources(json);
         Map<String, BackupManager.Resources> restoreParams = Collections.singletonMap(cacheManagerName, resources);
         return backupManager.restore(name, path, restoreParams);
      });
   }

   private CompletionStage<RestResponse> convertToJson(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());
      ContentSource contents = request.contents();

      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(new String(contents.rawContent(), UTF_8));
      Map.Entry<String, ConfigurationBuilder> entry = builderHolder.getNamedConfigurationBuilders().entrySet().iterator().next();
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(isPretty(request)).build()) {
         invocationHelper.getParserRegistry().serialize(w, entry.getKey(), entry.getValue().build());
      }
      responseBuilder.contentType(APPLICATION_JSON).entity(sw.toString());
      return completedFuture(responseBuilder.build());
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

   private CompletionStage<RestResponse> shutdown(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         Security.doAs(request.getSubject(), invocationHelper.getServer()::containerStop);
         return invocationHelper.newResponse(request).status(NO_CONTENT).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> listenConfig(RestRequest request) {
      return streamConfigurationAndEvents(request, false);
   }

   private CompletionStage<RestResponse> listenLifecycle(RestRequest request) {
      return streamConfigurationAndEvents(request, true);
   }

   private CompletionStage<RestResponse> streamConfigurationAndEvents(RestRequest request, boolean includeLifecycle) {
      MediaType mediaType = negotiateMediaType(request, APPLICATION_YAML, APPLICATION_JSON, APPLICATION_XML);
      boolean includeCurrentState = Boolean.parseBoolean(request.getParameter("includeCurrentState"));
      boolean pretty = isPretty(request);
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      ConfigurationListener listener = new ConfigurationListener(cacheManager, mediaType, includeCurrentState, pretty);
      responseBuilder.contentType(TEXT_EVENT_STREAM).entity(listener.getEventStream());

      CompletionStage<?> cs = SecurityActions.addListenerAsync(cacheManager, listener);
      if (includeLifecycle) {
         cs = cs.thenCompose(ignore -> SecurityActions.addLoggerListenerAsync(cacheManager, listener));
      }

      return cs.thenApply(v -> responseBuilder.build());
   }

   private String serializeConfig(Configuration cacheConfiguration, String name, MediaType mediaType, boolean pretty) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(mediaType).prettyPrint(pretty).build()) {
         parserRegistry.serialize(writer, null, Collections.singletonMap(name, cacheConfiguration));
      }
      return sw.toString();
   }

   private String serializeEvent(EventLog event, MediaType mediaType, boolean pretty) {
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).withType(mediaType).prettyPrint(pretty).build()) {
         parserRegistry.serializeWith(writer, new EventLogSerializer(), event);
      }
      return sw.toString();
   }

   @Listener
   public class ConfigurationListener {
      final EmbeddedCacheManager cacheManager;
      final EventStream eventStream;
      final MediaType mediaType;
      private final boolean pretty;

      protected ConfigurationListener(EmbeddedCacheManager cacheManager, MediaType mediaType, boolean includeCurrentState, boolean pretty) {
         this.cacheManager = cacheManager;
         this.mediaType = mediaType;
         this.pretty = pretty;
         this.eventStream = new EventStream(
               includeCurrentState ?
                     (stream) -> {
                        for (String configName : cacheManager.getCacheConfigurationNames()) {
                           Configuration config = SecurityActions.getCacheConfiguration(cacheManager, configName);
                           String eventType = config.isTemplate() ? "create-template" : "create-cache";
                           stream.sendEvent(new ServerSentEvent(eventType, serializeConfig(config, configName, mediaType, pretty)));
                        }
                     } : null,
               () -> Security.doPrivileged(() -> {
                  cacheManager.removeListenerAsync(this);
               }));
      }

      public EventStream getEventStream() {
         return eventStream;
      }

      @Logged
      public CompletionStage<Void> onDataLogged(EventLog event) {
         if (event.getCategory() != EventLogCategory.LIFECYCLE) return CompletableFutures.completedNull();

         final ServerSentEvent sse = new ServerSentEvent("lifecycle-event", serializeEvent(event, mediaType, pretty));
         return eventStream.sendEvent(sse);
      }

      @ConfigurationChanged
      public CompletionStage<Void> onConfigurationEvent(ConfigurationChangedEvent event) {
         String eventType = event.getConfigurationEventType().toString().toLowerCase() + "-" + event.getConfigurationEntityType();
         final ServerSentEvent sse;
         if (event.getConfigurationEventType() == ConfigurationChangedEvent.EventType.REMOVE) {
            sse = new ServerSentEvent(eventType, event.getConfigurationEntityName());
         } else {
            switch (event.getConfigurationEntityType()) {
               case "cache":
               case "template":
                     Configuration config = SecurityActions.getCacheConfiguration(cacheManager, event.getConfigurationEntityName());
                     sse = new ServerSentEvent(eventType, serializeConfig(config, event.getConfigurationEntityName(), mediaType, pretty));
                  break;
               default:
                  // Unhandled entity type, ignore
                  return CompletableFutures.completedNull();
            }
         }
         return eventStream.sendEvent(sse);
      }
   }
}
