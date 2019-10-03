package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.PUT;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.remoting.transport.Address;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.XSiteAdminOperations;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handles REST calls for cache level X-Site operations
 *
 * @since 10.0
 */
public class XSiteCacheResource implements ResourceHandler {

   private static final BiFunction<XSiteAdminOperations, String, String> TAKE_OFFLINE = XSiteAdminOperations::takeSiteOffline;
   private static final BiFunction<XSiteAdminOperations, String, String> BRING_ONLINE = XSiteAdminOperations::bringSiteOnline;
   private static final BiFunction<XSiteAdminOperations, String, String> PUSH_STATE = XSiteAdminOperations::pushState;
   private static final BiFunction<XSiteAdminOperations, String, String> CANCEL_PUSH_STATE = XSiteAdminOperations::cancelPushState;
   private static final BiFunction<XSiteAdminOperations, String, String> CANCEL_RECEIVE_STATE = XSiteAdminOperations::cancelReceiveState;
   private static final Function<XSiteAdminOperations, Map<String, String>> SITES_STATUS = XSiteAdminOperations::siteStatuses;
   private static final Function<XSiteAdminOperations, Map<String, String>> PUSH_STATE_STATUS = XSiteAdminOperations::getPushStateStatus;
   private static final Function<XSiteAdminOperations, String> CLEAR_PUSH_STATUS = XSiteAdminOperations::clearPushStateStatus;

   private final InvocationHelper invocationHelper;

   public XSiteCacheResource(InvocationHelper invocationHelper) {
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
            .create();
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
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
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
      if (takeOffline.afterFailures == current.afterFailures() && takeOffline.minTimeToWait == current.minTimeToWait()) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_MODIFIED.code()).build());
      }
      return CompletableFuture.supplyAsync(() -> {
         String status = xsiteAdmin.amendTakeOffline(site, takeOffline.afterFailures, takeOffline.minTimeToWait);
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

      try {
         byte[] payload = invocationHelper.getMapper().writeValueAsBytes(new TakeOffline(offlineStatus.getTakeOffline()));
         responseBuilder.entity(payload).contentType(APPLICATION_JSON_TYPE);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }

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
         try {
            byte[] statsResponse = invocationHelper.getMapper().writeValueAsBytes(payload);
            responseBuilder.contentType(APPLICATION_JSON).entity(statsResponse).status(OK);
         } catch (JsonProcessingException e) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private <T> CompletionStage<RestResponse> statusOperation(RestRequest request, Function<XSiteAdminOperations, T> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      XSiteAdminOperations xsiteAdmin = getxsiteAdmin(request);

      return CompletableFuture.supplyAsync(() -> {
         T payload = op.apply(xsiteAdmin);
         try {
            byte[] statsResponse = invocationHelper.getMapper().writeValueAsBytes(payload);
            responseBuilder.contentType(APPLICATION_JSON).entity(statsResponse).status(OK);
         } catch (JsonProcessingException e) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private XSiteAdminOperations getxsiteAdmin(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteAdminOperations.class);
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
   private static class TakeOffline {
      private int afterFailures;
      private long minTimeToWait;

      public void setAfterFailures(int afterFailures) {
         this.afterFailures = afterFailures;
      }

      public void setMinTimeToWait(long minTimeToWait) {
         this.minTimeToWait = minTimeToWait;
      }

      TakeOffline() {
      }

      TakeOffline(TakeOfflineConfiguration config) {
         this.afterFailures = config.afterFailures();
         this.minTimeToWait = config.minTimeToWait();
      }

      public int getAfterFailures() {
         return afterFailures;
      }

      public long getMinTimeToWait() {
         return minTimeToWait;
      }
   }
}
