package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.JsonUtils;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.status.AbstractMixedSiteStatus;
import org.infinispan.xsite.status.ContainerMixedSiteStatus;
import org.infinispan.xsite.status.OfflineSiteStatus;
import org.infinispan.xsite.status.OnlineSiteStatus;
import org.infinispan.xsite.status.SiteStatus;

/**
 * Handles REST calls for cache and cache manager level X-Site operations
 *
 * @since 10.0
 */
public class XSiteResource implements ResourceHandler {
   public static final String AFTER_FAILURES_FIELD = "after_failures";
   public static final String MIN_WAIT_FIELD = "min_wait";

   private static final BiFunction<XSiteAdminOperations, String, String> TAKE_OFFLINE = XSiteAdminOperations::takeSiteOffline;
   private static final BiFunction<XSiteAdminOperations, String, String> BRING_ONLINE = XSiteAdminOperations::bringSiteOnline;
   private static final BiFunction<XSiteAdminOperations, String, String> PUSH_STATE = XSiteAdminOperations::pushState;
   private static final BiFunction<XSiteAdminOperations, String, String> CANCEL_PUSH_STATE = XSiteAdminOperations::cancelPushState;
   private static final BiFunction<XSiteAdminOperations, String, String> CANCEL_RECEIVE_STATE = XSiteAdminOperations::cancelReceiveState;
   private static final Function<XSiteAdminOperations, Map<String, GlobalStatus>> SITES_STATUS =
         xSiteAdminOperations -> xSiteAdminOperations.clusterStatus().entrySet().stream()
               .collect(Collectors.toMap(Entry::getKey, GlobalStatus::fromSiteStatus));
   private static final Function<XSiteAdminOperations, Map<String, String>> PUSH_STATE_STATUS = XSiteAdminOperations::getPushStateStatus;
   private static final Function<XSiteAdminOperations, String> CLEAR_PUSH_STATUS = XSiteAdminOperations::clearPushStateStatus;
   private static final BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> BRING_ALL_CACHES_ONLINE = GlobalXSiteAdminOperations::bringAllCachesOnline;
   private static final BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> TAKE_ALL_CACHES_OFFLINE = GlobalXSiteAdminOperations::takeAllCachesOffline;
   private static final BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> START_PUSH_ALL_CACHES = GlobalXSiteAdminOperations::pushStateAllCaches;
   private static final BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> CANCEL_PUSH_ALL_CACHES = GlobalXSiteAdminOperations::cancelPushStateAllCaches;

   private final InvocationHelper invocationHelper;

   public XSiteResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("x-site", "Handles REST calls for cache and cache manager level X-Site operations")
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/local/").withAction("clear-push-state-status")
               .permission(AuthorizationPermission.ADMIN).name("XSITE PUSH STATE STATUS CLEAR").auditContext(AuditContext.CACHE)
               .handleWith(this::clearPushStateStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/")
               .permission(AuthorizationPermission.ADMIN).name("XSITE BACKUP STATUS").auditContext(AuditContext.CACHE)
               .handleWith(this::backupStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/").withAction("push-state-status")
               .permission(AuthorizationPermission.ADMIN).name("XSITE PUSH STATE STATUS").auditContext(AuditContext.CACHE)
               .handleWith(this::pushStateStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}")
               .permission(AuthorizationPermission.ADMIN).name("XSITE BACKUPS SITE STATUS").auditContext(AuditContext.CACHE)
               .handleWith(this::siteStatus)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("take-offline")
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE OFFLINE").auditContext(AuditContext.CACHE)
               .handleWith(this::takeSiteOffline)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("bring-online")
               .permission(AuthorizationPermission.ADMIN).name("XSITE BRING ONLINE").auditContext(AuditContext.CACHE)
               .handleWith(this::bringSiteOnline)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("start-push-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE START PUSH STATE").auditContext(AuditContext.CACHE)
               .handleWith(this::startStatePush)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("cancel-push-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE CANCEL PUSH STATE").auditContext(AuditContext.CACHE)
               .handleWith(this::cancelPushState)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}/take-offline-config")
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE OFFLINE CONFIG").auditContext(AuditContext.CACHE)
               .handleWith(this::getXSiteTakeOffline)
            .invocation().methods(PUT).path("/v2/caches/{cacheName}/x-site/backups/{site}/take-offline-config")
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE OFFLINE CONFIG UPDATE").auditContext(AuditContext.CACHE)
               .handleWith(this::updateTakeOffline)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("cancel-receive-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE CANCEL RECEIVE STATE").auditContext(AuditContext.CACHE)
               .handleWith(this::cancelReceiveState)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode")
               .permission(AuthorizationPermission.ADMIN).name("XSITE GET STATE TRANSFER MODE").auditContext(AuditContext.CACHE)
               .handleWith(this::getStateTransferMode)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/x-site/backups/{site}/state-transfer-mode").withAction("set")
               .permission(AuthorizationPermission.ADMIN).name("XSITE SET STATE TRANSFER MODE").auditContext(AuditContext.CACHE)
               .handleWith(this::setStateTransferMode)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE GLOBAL STATUS").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::globalStatus)
            .invocation().methods(GET).path("/v2/container/x-site/backups/")
               .permission(AuthorizationPermission.ADMIN).name("XSITE GLOBAL STATUS").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::globalStatus)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/{site}")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE GLOBAL SITE STATUS").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::globalStatus)
            .invocation().methods(GET).path("/v2/container/x-site/backups/{site}")
               .permission(AuthorizationPermission.ADMIN).name("XSITE GLOBAL SITE STATUS").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::globalStatus)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("bring-online")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE BRING ALL ONLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::bringAllOnline)
            .invocation().methods(POST).path("/v2/container/x-site/backups/{site}").withAction("bring-online")
               .permission(AuthorizationPermission.ADMIN).name("XSITE BRING ALL ONLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::bringAllOnline)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("take-offline")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE ALL OFFLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::takeAllOffline)
            .invocation().methods(POST).path("/v2/container/x-site/backups/{site}").withAction("take-offline")
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE ALL OFFLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::takeAllOffline)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("start-push-state")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE START PUSH ALL").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::startPushAll)
            .invocation().methods(POST).path("/v2/container/x-site/backups/{site}").withAction("start-push-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE START PUSH ALL").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::startPushAll)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("cancel-push-state")
               .deprecated()
               .permission(AuthorizationPermission.ADMIN).name("XSITE CANCEL PUSH ALL").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::cancelPushAll)
            .invocation().methods(POST).path("/v2/container/x-site/backups/{site}").withAction("cancel-push-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE CANCEL PUSH ALL").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::cancelPushAll)
            .create();
   }

   private CompletionStage<RestResponse> bringAllOnline(RestRequest request) {
      return executeCacheManagerXSiteOp(request, BRING_ALL_CACHES_ONLINE);
   }

   private CompletionStage<RestResponse> takeAllOffline(RestRequest request) {
      return executeCacheManagerXSiteOp(request, TAKE_ALL_CACHES_OFFLINE);
   }

   private CompletionStage<RestResponse> startPushAll(RestRequest request) {
      return executeCacheManagerXSiteOp(request, START_PUSH_ALL_CACHES);
   }

   private CompletionStage<RestResponse> cancelPushAll(RestRequest request) {
      return executeCacheManagerXSiteOp(request, CANCEL_PUSH_ALL_CACHES);
   }

   private CompletionStage<RestResponse> globalStatus(RestRequest request) {
      GlobalXSiteAdminOperations globalXSiteAdmin = getGlobalXSiteAdmin(request);
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);

      if (globalXSiteAdmin == null) return completedFuture(responseBuilder.status(NOT_FOUND).build());

      return supplyAsync(() -> {
         Map<String, SiteStatus> globalStatus = Security.doAs(request.getSubject(), globalXSiteAdmin::globalStatus);
         Map<String, GlobalStatus> collect = globalStatus.entrySet().stream().collect(Collectors.toMap(Entry::getKey, GlobalStatus::fromSiteStatus));
         String site = request.variables().get("site");
         if (site != null) {
            GlobalStatus siteStatus = collect.get(site);
            return siteStatus == null ?
                  responseBuilder.status(NOT_FOUND).build() :
                  addEntityAsJson(Json.make(siteStatus), responseBuilder, isPretty(request)).build();
         }
         return addEntityAsJson(Json.make(collect), responseBuilder, isPretty(request)).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> pushStateStatus(RestRequest request) {
      return statusOperation(request, PUSH_STATE_STATUS);
   }

   private CompletionStage<RestResponse> backupStatus(RestRequest request) {
      return statusOperation(request, SITES_STATUS);
   }

   private CompletionStage<RestResponse> clearPushStateStatus(RestRequest restRequest) {
      return statusOperation(restRequest, CLEAR_PUSH_STATUS);
   }

   private CompletionStage<RestResponse> cancelReceiveState(RestRequest restRequest) {
      return executeXSiteCacheOp(restRequest, CANCEL_RECEIVE_STATE);
   }

   private CompletionStage<RestResponse> cancelPushState(RestRequest restRequest) {
      return executeXSiteCacheOp(restRequest, CANCEL_PUSH_STATE);
   }

   private CompletionStage<RestResponse> startStatePush(RestRequest restRequest) {
      return executeXSiteCacheOp(restRequest, PUSH_STATE);
   }

   private CompletionStage<RestResponse> takeSiteOffline(RestRequest request) {
      return executeXSiteCacheOp(request, TAKE_OFFLINE);
   }

   private CompletionStage<RestResponse> bringSiteOnline(RestRequest request) {
      return executeXSiteCacheOp(request, BRING_ONLINE);
   }

   private CompletionStage<RestResponse> updateTakeOffline(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request).status(NO_CONTENT);
      String site = request.variables().get("site");

      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);
      if (!xsiteAdminOpt.isPresent()) {
         return completedFuture(responseBuilder.build());
      }

      XSiteAdminOperations xsiteAdmin = xsiteAdminOpt.get();
      TakeOfflineConfiguration current = xsiteAdmin.getTakeOfflineConfiguration(site);
      assert current != null;
      String content = request.contents().asString();
      if (content == null || content.isEmpty()) {
         throw Log.REST.missingContent();
      }

      int afterFailures, minWait;

      Json json = Json.read(content);
      Json minWaitValue = json.at(MIN_WAIT_FIELD);
      Json afterFailuresValue = json.at(AFTER_FAILURES_FIELD);
      if (minWaitValue == null || afterFailuresValue == null) {
         throw Log.REST.missingArguments(MIN_WAIT_FIELD, AFTER_FAILURES_FIELD);
      }
      minWait = minWaitValue.asInteger();
      afterFailures = afterFailuresValue.asInteger();
      if (afterFailures == current.afterFailures() && minWait == current.minTimeToWait()) {
         return completedFuture(responseBuilder.status(NOT_MODIFIED).build());
      }
      return supplyAsync(() -> {
         String status = Security.doAs(request.getSubject(), () -> xsiteAdmin.amendTakeOffline(site, afterFailures, minWait));
         if (!status.equals(XSiteAdminOperations.SUCCESS)) {
            throw Log.REST.siteOperationFailed(site, status);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getXSiteTakeOffline(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String site = request.variables().get("site");

      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);
      xsiteAdminOpt.ifPresent(ops -> {
         TakeOfflineConfiguration config = ops.getTakeOfflineConfiguration(site);
         assert config != null;
         addEntityAsJson(new TakeOffline(config), responseBuilder);
      });
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> siteStatus(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String site = request.variables().get("site");

      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);
      return xsiteAdminOpt.<CompletionStage<RestResponse>>map(ops -> supplyAsync(
            () -> addEntityAsJson(Json.make(Security.doAs(request.getSubject(), () -> ops.nodeStatus(site))), responseBuilder, isPretty(request)).build(),
            invocationHelper.getExecutor()))
            .orElseGet(() -> completedFuture(responseBuilder.build()));

   }

   private <T> CompletionStage<RestResponse> statusOperation(RestRequest request, Function<XSiteAdminOperations, T> op) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      Optional<XSiteAdminOperations> xsiteAdmin = getXSiteAdmin(request, responseBuilder);
      return xsiteAdmin.<CompletionStage<RestResponse>>map(ops -> supplyAsync(
            () -> {
               T result = Security.doAs(request.getSubject(), () -> op.apply(ops));
               return addEntityAsJson(Json.make(result), responseBuilder, isPretty(request)).build();
            },
            invocationHelper.getExecutor()))
            .orElseGet(() -> completedFuture(responseBuilder.build()));
   }

   private CompletionStage<RestResponse> getStateTransferMode(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);

      //check if site exists
      final String site = request.variables().get("site");
      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);

      return xsiteAdminOpt.<CompletionStage<RestResponse>>map(ops ->
             ops.asyncGetStateTransferMode(site).thenApply(s -> addEntityAsJson(Json.make(s), responseBuilder, isPretty(request)).build()))
            .orElseGet(() -> completedFuture(responseBuilder.build()));
   }

   private CompletionStage<RestResponse> setStateTransferMode(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      //parse content
      String mode = request.getParameter("mode");
      if (mode == null) {
         throw Log.REST.missingArgument("mode");
      }
      //check if site exists
      final String site = request.variables().get("site");
      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);
      return xsiteAdminOpt.<CompletionStage<RestResponse>>map(ops ->
            ops.asyncSetStateTransferMode(site, mode)
                  .thenApply(ok -> responseBuilder.status(ok ? OK : NOT_MODIFIED).build()))
            .orElseGet(() -> completedFuture(responseBuilder.build()));

   }

   private Optional<XSiteAdminOperations> getXSiteAdmin(RestRequest request, NettyRestResponse.Builder responseBuilder) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      XSiteAdminOperations ops = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
      if (ops == null) {
         noBackupsForCache(responseBuilder, cacheName);
         return Optional.empty();
      }
      return Optional.of(ops);
   }

   private Optional<XSiteAdminOperations> getXSiteAdminAndCheckSite(RestRequest request, NettyRestResponse.Builder responseBuilder) {
      String cacheName = request.variables().get("cacheName");
      String site = request.variables().get("site");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      XSiteAdminOperations ops = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache()).getComponent(XSiteAdminOperations.class);
      if (ops == null) {
         noBackupsForCache(responseBuilder, cacheName);
         return Optional.empty();
      }
      if (!ops.checkSite(site)) {
         siteNotFound(responseBuilder, cacheName, site);
         return Optional.empty();
      }
      return Optional.of(ops);
   }

   private GlobalXSiteAdminOperations getGlobalXSiteAdmin(RestRequest request) {
      EmbeddedCacheManager cm = invocationHelper.getRestCacheManager().getInstance();
      return SecurityActions.getGlobalComponentRegistry(cm).getComponent(GlobalXSiteAdminOperations.class);
   }

   private CompletionStage<RestResponse> executeCacheManagerXSiteOp(RestRequest request,
                                                                    BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> operation) {
      GlobalXSiteAdminOperations globalXSiteAdmin = getGlobalXSiteAdmin(request);
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);

      String site = request.variables().get("site");

      if (globalXSiteAdmin == null) return completedFuture(responseBuilder.status(NOT_FOUND).build());

      return supplyAsync(
            () -> {
               Map<String, String> result = Security.doAs(request.getSubject(), operation, globalXSiteAdmin, site);
               return addEntityAsJson(Json.make(result), responseBuilder, isPretty(request)).build();
            },
            invocationHelper.getExecutor()
      );
   }

   private CompletionStage<RestResponse> executeXSiteCacheOp(RestRequest request, BiFunction<XSiteAdminOperations, String, String> xsiteOp) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String site = request.variables().get("site");

      Optional<XSiteAdminOperations> xsiteAdminOpt = getXSiteAdminAndCheckSite(request, responseBuilder);
      return xsiteAdminOpt.<CompletionStage<RestResponse>>map(ops ->
            supplyAsync(() -> {
               String result = Security.doAs(request.getSubject(), () -> xsiteOp.apply(ops, site));
               if (!result.equals(XSiteAdminOperations.SUCCESS)) {
                  throw Log.REST.siteOperationFailed(site, result);
               }
               return responseBuilder.build();
            }, invocationHelper.getExecutor()))
            .orElseGet(() -> completedFuture(responseBuilder.build()));

   }

   private static void noBackupsForCache(NettyRestResponse.Builder builder, String cacheName) {
      builder.status(NOT_FOUND).contentType(MediaType.TEXT_PLAIN).entity(String.format("Cache '%s' does not have backup sites.", cacheName));
   }

   private static void siteNotFound(NettyRestResponse.Builder builder, String cacheName, String site) {
      builder.status(NOT_FOUND).contentType(MediaType.TEXT_PLAIN).entity(String.format("Cache '%s' does not backup to site '%s'", cacheName, site));
   }

   private static class GlobalStatus implements JsonSerialization {
      static final GlobalStatus OFFLINE = new GlobalStatus("offline", null, null, null);
      static final GlobalStatus ONLINE = new GlobalStatus("online", null, null, null);
      static final GlobalStatus UNKNOWN = new GlobalStatus("unknown", null, null, null);

      private final String status;
      private final Json online;
      private final Json offline;
      private final Json mixed;

      GlobalStatus(String status, Json online, Json offline, Json mixed) {
         this.status = status;
         this.online = online;
         this.offline = offline;
         this.mixed = mixed;
      }

      static GlobalStatus mixed(Json online, Json offline, Json mixed) {
         return new GlobalStatus("mixed", online, offline, mixed);
      }

      static GlobalStatus fromSiteStatus(Map.Entry<String, SiteStatus> entry) {
         SiteStatus status = entry.getValue();
         if (status instanceof OnlineSiteStatus) return GlobalStatus.ONLINE;
         if (status instanceof OfflineSiteStatus) return GlobalStatus.OFFLINE;
         Json mixed = null;
         if (status instanceof ContainerMixedSiteStatus) {
            mixed = JsonUtils.createJsonArray(((ContainerMixedSiteStatus) status).getMixedCaches());
         }
         if (status instanceof AbstractMixedSiteStatus<?>) {
            Json online = JsonUtils.createJsonArray(((AbstractMixedSiteStatus<?>) status).getOnline().stream().map(String::valueOf));
            Json offline =JsonUtils.createJsonArray(((AbstractMixedSiteStatus<?>) status).getOffline().stream().map(String::valueOf));
            return GlobalStatus.mixed(online, offline, mixed);
         }
         return GlobalStatus.UNKNOWN;
      }

      @Override
      public Json toJson() {
         Json json = Json.object().set("status", this.status);
         if (online != null) {
            json.set("online", online);
         }
         if (offline != null) {
            json.set("offline", offline);
         }
         if (mixed != null) {
            json.set("mixed", mixed);
         }
         return json;
      }
   }

   private static class TakeOffline implements JsonSerialization {
      private final int afterFailures;
      private final long minWait;

      TakeOffline(TakeOfflineConfiguration config) {
         this.afterFailures = config.afterFailures();
         this.minWait = config.minTimeToWait();
      }

      @Override
      public Json toJson() {
         return Json.object()
               .set(AFTER_FAILURES_FIELD, afterFailures)
               .set(MIN_WAIT_FIELD, minWait);
      }
   }
}
