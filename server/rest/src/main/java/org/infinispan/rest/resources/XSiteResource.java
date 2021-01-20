package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.xsite.XSiteAdminOperations.siteStatusToString;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.status.AbstractMixedSiteStatus;
import org.infinispan.xsite.status.OfflineSiteStatus;
import org.infinispan.xsite.status.OnlineSiteStatus;
import org.infinispan.xsite.status.SiteStatus;

import io.netty.handler.codec.http.HttpResponseStatus;

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
   private static final Function<XSiteAdminOperations, Map<String, String>> SITES_STATUS = xSiteAdminOperations -> {
      Map<String, SiteStatus> statuses = xSiteAdminOperations.clusterStatus();
      return statuses.entrySet()
            .stream()
            .collect(Collectors.toMap(Entry::getKey, e -> siteStatusToString(e.getValue())));
   };
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
      return new Invocations.Builder()
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
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/")
               .permission(AuthorizationPermission.ADMIN).name("XSITE GLOBAL STATUS").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::globalStatus)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("bring-online")
               .permission(AuthorizationPermission.ADMIN).name("XSITE BRING ALL ONLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::bringAllOnline)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("take-offline")
               .permission(AuthorizationPermission.ADMIN).name("XSITE TAKE ALL OFFLINE").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::takeAllOffline)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("start-push-state")
               .permission(AuthorizationPermission.ADMIN).name("XSITE START PUSH ALL").auditContext(AuditContext.CACHEMANAGER)
               .handleWith(this::startPushAll)
            .invocation().methods(POST).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("cancel-push-state")
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
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      if (globalXSiteAdmin == null) return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND).build());

      return CompletableFuture.supplyAsync(() -> {
         Map<String, SiteStatus> globalStatus = Security.doAs(request.getSubject(), (PrivilegedAction<Map<String, SiteStatus>>) globalXSiteAdmin::globalStatus);
         Map<String, GlobalStatus> collect = globalStatus.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
            SiteStatus status = e.getValue();
            if (status instanceof OnlineSiteStatus) return GlobalStatus.ONLINE;
            if (status instanceof OfflineSiteStatus) return GlobalStatus.OFFLINE;
            if (status instanceof AbstractMixedSiteStatus<?>) {
               AbstractMixedSiteStatus<?> mixedSiteStatus = (AbstractMixedSiteStatus<?>) status;
               return GlobalStatus.mixed(mixedSiteStatus.getOnline(), mixedSiteStatus.getOffline());
            }
            return GlobalStatus.UNKNOWN;
         }));
         return addEntityAsJson(Json.make(collect), responseBuilder).build();
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
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder().status(NO_CONTENT);
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getXSiteAdmin(request);
      TakeOfflineConfiguration current = Security.doAs(request.getSubject(), (PrivilegedAction<TakeOfflineConfiguration>) () -> xsiteAdmin.getTakeOfflineConfiguration(site));

      if (current == null) {
         return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND).build());
      }
      String content = request.contents().asString();
      if (content == null || content.isEmpty()) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.BAD_REQUEST).build());
      }

      int afterFailures, minWait;
      try {
         Json json = Json.read(content);
         Json minWaitValue = json.at(MIN_WAIT_FIELD);
         Json afterFailuresValue = json.at(AFTER_FAILURES_FIELD);
         if (minWaitValue == null || afterFailuresValue == null) {
            return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.BAD_REQUEST).build());
         }
         minWait = minWaitValue.asInteger();
         afterFailures = afterFailuresValue.asInteger();
      } catch (Exception e) {
         Throwable rootCause = Util.getRootCause(e);
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(rootCause.getMessage()).build());
      }
      if (afterFailures == current.afterFailures() && minWait == current.minTimeToWait()) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_MODIFIED).build());
      }
      return CompletableFuture.supplyAsync(() -> {
         String status = Subject.doAs(request.getSubject(), (PrivilegedAction<String>) () -> xsiteAdmin.amendTakeOffline(site, afterFailures, minWait));
         if (!status.equals(XSiteAdminOperations.SUCCESS)) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(site);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getXSiteTakeOffline(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getXSiteAdmin(request);
      TakeOfflineConfiguration config = xsiteAdmin.getTakeOfflineConfiguration(site);
      if (config == null) {
         return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND).build());
      }

      return completedFuture(addEntityAsJson(new TakeOffline(config), responseBuilder).build()
      );
   }

   private CompletionStage<RestResponse> siteStatus(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getXSiteAdmin(request);

      if (!xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND).build());
      }

      return CompletableFuture.supplyAsync(
            () -> {
               Map<Address, String> result = Subject.doAs(request.getSubject(), (PrivilegedAction<Map<Address, String>>) () -> xsiteAdmin.nodeStatus(site));
               return addEntityAsJson(Json.make(result), responseBuilder).build();
            }
            , invocationHelper.getExecutor());
   }

   private <T> CompletionStage<RestResponse> statusOperation(RestRequest request, Function<XSiteAdminOperations, T> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      XSiteAdminOperations xsiteAdmin = getXSiteAdmin(request);

      return CompletableFuture.supplyAsync(
            () -> {
               T result = Security.doAs(request.getSubject(), op, xsiteAdmin);
               return addEntityAsJson(Json.make(result), responseBuilder).build();
            },
            invocationHelper.getExecutor());
   }

   private XSiteAdminOperations getXSiteAdmin(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
   }

   private GlobalXSiteAdminOperations getGlobalXSiteAdmin(RestRequest request) {
      String cacheManager = request.variables().get("name");
      EmbeddedCacheManager cm = invocationHelper.getRestCacheManager().getInstance();

      if (!cacheManager.equals(cm.getCacheManagerInfo().getName())) return null;

      return SecurityActions.getGlobalComponentRegistry(cm).getComponent(GlobalXSiteAdminOperations.class);
   }

   private CompletionStage<RestResponse> executeCacheManagerXSiteOp(RestRequest request,
                                                                    BiFunction<GlobalXSiteAdminOperations, String, Map<String, String>> operation) {
      GlobalXSiteAdminOperations globalXSiteAdmin = getGlobalXSiteAdmin(request);
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      String site = request.variables().get("site");

      if (globalXSiteAdmin == null) return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND).build());

      return CompletableFuture.supplyAsync(
            () -> {
               Map<String, String> result = Security.doAs(request.getSubject(), operation, globalXSiteAdmin, site);
               return addEntityAsJson(Json.make(result), responseBuilder).build();
            },
            invocationHelper.getExecutor()
      );
   }

   private CompletionStage<RestResponse> executeXSiteCacheOp(RestRequest request, BiFunction<XSiteAdminOperations, String, String> xsiteOp) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getXSiteAdmin(request);

      if (xsiteAdmin == null || !xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND).build());
      }

      return CompletableFuture.supplyAsync(() -> {
         String result = Security.doAs(request.getSubject(), xsiteOp, xsiteAdmin, site);
         if (!result.equals(XSiteAdminOperations.SUCCESS)) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(result);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private static class GlobalStatus implements JsonSerialization {
      static final GlobalStatus OFFLINE = new GlobalStatus("offline", null, null);
      static final GlobalStatus ONLINE = new GlobalStatus("online", null, null);
      static final GlobalStatus UNKNOWN = new GlobalStatus("unknown", null, null);

      private final String status;
      private final List<?> online;
      private final List<?> offline;

      GlobalStatus(String status, List<?> online, List<?> offline) {
         this.status = status;
         this.online = online;
         this.offline = offline;
      }

      static GlobalStatus mixed(List<?> online, List<?> offline) {
         return new GlobalStatus("mixed", online, offline);
      }

      @Override
      public Json toJson() {
         Json json = Json.object().set("status", this.status);
         if (online != null) {
            List<String> onLines = online.stream().map(Object::toString).collect(Collectors.toList());
            json.set("online", Json.make(onLines));
         }
         if (offline != null) {
            List<String> offLines = offline.stream().map(Object::toString).collect(Collectors.toList());
            json.set("offline", Json.make(offLines));
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
