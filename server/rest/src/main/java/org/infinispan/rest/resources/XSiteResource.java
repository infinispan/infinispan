package org.infinispan.rest.resources;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.xsite.XSiteAdminOperations.siteStatusToString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.xsite.GlobalXSiteAdminOperations;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.XSiteAdminOperations;
import org.infinispan.xsite.status.AbstractMixedSiteStatus;
import org.infinispan.xsite.status.OfflineSiteStatus;
import org.infinispan.xsite.status.OnlineSiteStatus;
import org.infinispan.xsite.status.SiteStatus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handles REST calls for cache and cache manager level X-Site operations
 *
 * @since 10.0
 */
public class XSiteResource implements ResourceHandler {

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
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/local/").withAction("clear-push-state-status").handleWith(this::clearPushStateStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/").handleWith(this::backupStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/").withAction("push-state-status").handleWith(this::pushStateStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").handleWith(this::siteStatus)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("take-offline").handleWith(this::takeSiteOffline)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("bring-online").handleWith(this::bringSiteOnline)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("start-push-state").handleWith(this::startStatePush)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("cancel-push-state").handleWith(this::cancelPushState)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}/take-offline-config").handleWith(this::getXSiteTakeOffline)
            .invocation().methods(PUT).path("/v2/caches/{cacheName}/x-site/backups/{site}/take-offline-config").handleWith(this::updateTakeOffline)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/x-site/backups/{site}").withAction("cancel-receive-state").handleWith(this::cancelReceiveState)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/").handleWith(this::globalStatus)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("bring-online").handleWith(this::bringAllOnline)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("take-offline").handleWith(this::takeAllOffline)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("start-push-state").handleWith(this::startPushAll)
            .invocation().methods(GET).path("/v2/cache-managers/{name}/x-site/backups/{site}").withAction("cancel-push-state").handleWith(this::cancelPushAll)
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
         Map<String, GlobalStatus> collect = globalXSiteAdmin.globalStatus().entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
            SiteStatus status = e.getValue();
            if (status instanceof OnlineSiteStatus) return GlobalStatus.ONLINE;
            if (status instanceof OfflineSiteStatus) return GlobalStatus.OFFLINE;
            if (status instanceof AbstractMixedSiteStatus) {
               AbstractMixedSiteStatus mixedSiteStatus = (AbstractMixedSiteStatus) status;
               return GlobalStatus.mixed(mixedSiteStatus.getOnline(), mixedSiteStatus.getOffline());
            }
            return GlobalStatus.UNKNOWN;
         }));
         addPayload(responseBuilder, collect);
         return responseBuilder.build();
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

      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);

      if (!xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND.code()).build());
      }
      byte[] byteContent = request.contents().rawContent();
      if (byteContent == null || byteContent.length == 0) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.BAD_REQUEST.code()).build());
      }

      TakeOfflineConfiguration current = xsiteAdmin.getOfflineStatus(site).getTakeOffline();
      TakeOffline takeOffline;
      try {
         takeOffline = invocationHelper.getMapper().readValue(byteContent, TakeOffline.class);
      } catch (IOException e) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.BAD_REQUEST).build());
      }
      if (takeOffline.afterFailures == current.afterFailures() && takeOffline.minWait == current.minTimeToWait()) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_MODIFIED.code()).build());
      }
      return CompletableFuture.supplyAsync(() -> {
         String status = xsiteAdmin.amendTakeOffline(site, takeOffline.afterFailures, takeOffline.minWait);
         if (!status.equals(XSiteAdminOperations.SUCCESS)) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).entity(site);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getXSiteTakeOffline(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);
      if (!xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(NOT_FOUND.code()).build());
      }
      OfflineStatus offlineStatus = xsiteAdmin.getOfflineStatus(site);

      addPayload(responseBuilder, new TakeOffline(offlineStatus.getTakeOffline()));

      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> siteStatus(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);

      if (!xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).build());
      }

      return CompletableFuture.supplyAsync(() -> {
         Map<Address, String> payload = xsiteAdmin.nodeStatus(site);
         addPayload(responseBuilder, payload);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private <T> CompletionStage<RestResponse> statusOperation(RestRequest request, Function<XSiteAdminOperations, T> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);

      return CompletableFuture.supplyAsync(() -> {
         T payload = op.apply(xsiteAdmin);
         addPayload(responseBuilder, payload);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private XSiteAdminOperations getxsiteAdmin(RestRequest request) {
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

      return CompletableFuture.supplyAsync(() -> {
         Map<String, String> payload = operation.apply(globalXSiteAdmin, site);
         addPayload(responseBuilder, payload);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private void addPayload(NettyRestResponse.Builder responseBuilder, Object o) {
      try {
         byte[] statsResponse = invocationHelper.getMapper().writeValueAsBytes(o);
         responseBuilder.contentType(APPLICATION_JSON).entity(statsResponse).status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
   }


   private CompletionStage<RestResponse> executeXSiteCacheOp(RestRequest request, BiFunction<XSiteAdminOperations, String, String> xsiteOp) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String site = request.variables().get("site");

      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);

      if (!xsiteAdmin.checkSite(site)) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).build());
      }

      return CompletableFuture.supplyAsync(() -> {
         String result = xsiteOp.apply(xsiteAdmin, site);
         if (!result.equals(XSiteAdminOperations.SUCCESS)) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).entity(result);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   @SuppressWarnings("unused")
   private static class GlobalStatus {
      static final GlobalStatus OFFLINE = new GlobalStatus("offline", null, null);
      static final GlobalStatus ONLINE = new GlobalStatus("online", null, null);
      static final GlobalStatus UNKNOWN = new GlobalStatus("unknown", null, null);

      private String status;
      private List online;
      private List offline;

      GlobalStatus(String status, List online, List offline) {
         this.status = status;
         this.online = online;
         this.offline = offline;
      }

      static GlobalStatus mixed(List online, List offline) {
         return new GlobalStatus("mixed", online, offline);
      }

      public String getStatus() {
         return status;
      }

      @JsonInclude(NON_NULL)
      public List getOnline() {
         return online;
      }

      @JsonInclude(NON_NULL)
      public List getOffline() {
         return offline;
      }
   }

   @SuppressWarnings("unused")
   private static class TakeOffline {
      private int afterFailures;
      private long minWait;

      public void setAfterFailures(int afterFailures) {
         this.afterFailures = afterFailures;
      }

      public void setMinWait(long minWait) {
         this.minWait = minWait;
      }

      TakeOffline() {
      }

      TakeOffline(TakeOfflineConfiguration config) {
         this.afterFailures = config.afterFailures();
         this.minWait = config.minTimeToWait();
      }

      public int getAfterFailures() {
         return afterFailures;
      }

      public long getMinWait() {
         return minWait;
      }
   }
}
