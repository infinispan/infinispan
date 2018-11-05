package org.infinispan.rest.authentication.impl;

import org.infinispan.rest.NettyRestRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.Authenticator;

import io.netty.channel.ChannelHandlerContext;

/**
 * Accept all authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public class VoidAuthenticator implements Authenticator {

   @Override
   public void challenge(NettyRestRequest request, ChannelHandlerContext ctx) throws RestResponseException {
   }
}
