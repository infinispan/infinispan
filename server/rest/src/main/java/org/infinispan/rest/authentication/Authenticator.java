package org.infinispan.rest.authentication;

import org.infinispan.rest.NettyRestRequest;
import org.infinispan.rest.RestResponseException;

import io.netty.channel.ChannelHandlerContext;

/**
 * Authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public interface Authenticator {

   /**
    * Challenges specific {@link NettyRestRequest} for authentication.
    *
    * @param request Request to be challenged.
    * @throws RestResponseException Thrown on error.
    * @throws AuthenticationException Thrown if authentication fails.
    */
   void challenge(NettyRestRequest request, ChannelHandlerContext ctx) throws RestResponseException;

}
