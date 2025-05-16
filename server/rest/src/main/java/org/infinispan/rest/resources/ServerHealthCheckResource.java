package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.security.actions.SecurityActions;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ServerHealthCheckResource implements ResourceHandler {

   private final InvocationHelper helper;

   public ServerHealthCheckResource(InvocationHelper helper) {
      this.helper = helper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()

            .invocation().methods(GET, HEAD).path("/health/live")
            .handleWith(this::notifyServerRunning)

            .invocation().methods(GET, HEAD).path("/health/ready")
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
      if (dcm.getStatus().allowInvocations()) {
         if (dcm.getCacheNames().stream().anyMatch(this::isCacheReady))
            status = HttpResponseStatus.OK;
      }

      return CompletableFuture.completedFuture(builder.status(status).build());
   }

   private boolean isCacheReady(String cacheName) {
      try {
         GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(helper.getServer().getCacheManager());
         ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

         // Component registry will be null during a graceful shutdown.
         // This method does not receive the cache name from a user invocation, it is fine to accept null as ready since it uses the define names.
         return cr == null || cr.getStatus() != ComponentStatus.FAILED;
      } catch (CacheException cacheException) {
         return false;
      }
   }
}
