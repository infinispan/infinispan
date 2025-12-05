package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import io.netty.handler.codec.http.HttpResponseStatus;

public class HealthCheckResource implements ResourceHandler {

   private final InvocationHelper helper;

   public HealthCheckResource(InvocationHelper helper) {
      this.helper = helper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("health", "Health")

            .invocation().methods(GET, HEAD).anonymous(true).path("/health/live").requireCacheManagerStart(false)
            .handleWith(this::notifyServerRunning)

            .invocation().methods(GET, HEAD).anonymous(true).path("/health/ready").requireCacheManagerStart(false)
            .handleWith(this::verifyServerReady)

            .create();
   }

   public CompletionStage<RestResponse> notifyServerRunning(RestRequest request) {
      NettyRestResponse.Builder builder = helper.newResponse(request);
      return CompletableFuture.completedFuture(builder.status(HttpResponseStatus.OK).build());
   }

   public CompletionStage<RestResponse> verifyServerReady(RestRequest request) {
      NettyRestResponse.Builder builder = helper.newResponse(request);
      DefaultCacheManager dcm = helper.getServer().getCacheManager();
      HttpResponseStatus status = HttpResponseStatus.SERVICE_UNAVAILABLE;
      if (dcm.getStatus().allowInvocations() && helper.getProtocolServer().isStarted()) {
         status = HttpResponseStatus.OK;
         CacheManagerInfo cmi = dcm.getCacheManagerInfo();
         if (cmi.allCachesStopped())
            status = HttpResponseStatus.SERVICE_UNAVAILABLE;
      }

      return CompletableFuture.completedFuture(builder.status(status).build());
   }
}
