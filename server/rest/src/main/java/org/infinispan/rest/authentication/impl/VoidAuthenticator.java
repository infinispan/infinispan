package org.infinispan.rest.authentication.impl;

import java.util.concurrent.CompletionStage;

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
      return COMPLETED_VOID_RESPONSE;
   }
}
