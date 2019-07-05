package org.infinispan.rest.authentication.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * Accept all authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public class VoidAuthenticator implements Authenticator {
   @Override
   public CompletionStage<RestResponse> challenge(RestRequest request, ChannelHandlerContext ctx) {
      return CompletableFuture.completedFuture(new NettyRestResponse.Builder().build());
   }
}
