package org.infinispan.rest.authentication.impl;

import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.SecurityDomain;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * BASIC authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class BasicAuthenticator implements Authenticator {

   private final SecurityDomain domain;
   private final String authenticateHeader;
   private Executor executor;

   public BasicAuthenticator(SecurityDomain domain, String realm) {
      this.domain = domain;
      this.authenticateHeader = realm != null ? String.format("Basic realm=\"%s\"", realm) : "Basic";
   }

   @Override
   public CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx) {
      String auth = request.getAuthorizationHeader();
      if (auth != null && auth.length() > 5) {
         String type = auth.substring(0, 5);
         if ("basic".equalsIgnoreCase(type)) {
            String cookie = auth.substring(6);
            cookie = new String(Base64.getDecoder().decode(cookie.getBytes()));
            String[] split = cookie.split(":");
            return CompletableFuture.supplyAsync(() -> {
               request.setSubject(domain.authenticate(split[0], split[1]));
               return new NettyRestResponse.Builder().build();
            }, executor);
         }
      }
      return CompletableFuture.completedFuture(new NettyRestResponse.Builder().status(HttpResponseStatus.UNAUTHORIZED).authenticate(authenticateHeader).build());
   }

   @Override
   public void init(RestServer restServer) {
      executor = restServer.getExecutor();
   }
}
