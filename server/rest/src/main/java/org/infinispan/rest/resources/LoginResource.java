package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class LoginResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final String accessGrantedPath;
   private final String accessDeniedPath;

   public LoginResource(InvocationHelper invocationHelper, String accessGrantedPath, String accessDeniedPath) {
      this.invocationHelper = invocationHelper;
      this.accessGrantedPath = accessGrantedPath;
      this.accessDeniedPath = accessDeniedPath;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/login").withAction("config").anonymous(true).handleWith(this::loginConfiguration)
            .invocation().methods(GET, POST).deprecated().path("/v2/login").withAction("login").handleWith(this::login)
            .invocation().methods(GET).path("/v2/login").handleWith(this::login)
            .create();
   }

   private CompletionStage<RestResponse> loginConfiguration(RestRequest restRequest) {
      Map<String, String> loginConfiguration = invocationHelper.getServer().getLoginConfiguration(invocationHelper.getProtocolServer());
      return asJsonResponseFuture(Json.make(loginConfiguration));
   }

   private CompletionStage<RestResponse> login(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      responseBuilder.status(HttpResponseStatus.TEMPORARY_REDIRECT).header("Location", accessGrantedPath);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }
}
