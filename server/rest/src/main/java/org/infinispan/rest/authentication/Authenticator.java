package org.infinispan.rest.authentication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * Authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public interface Authenticator {

   RestResponse VOID_RESPONSE = new NettyRestResponse.Builder().build();

   CompletionStage<RestResponse> COMPLETED_VOID_RESPONSE = CompletableFuture.completedFuture(VOID_RESPONSE);

   /**
    * Challenges specific {@link RestRequest} for authentication.
    *
    * @param request Request to be challenged.
    * @return a {@link RestResponse} wrapped in a {@link CompletionStage}
    */
   CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx);

   /**
    * Invoked by the {@link RestServer} on startup. Can perform additional configuration
    * @param restServer
    */
   default void init(RestServer restServer) {}
}
