package org.infinispan.rest.authentication;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.rest.RestServer;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;

import io.netty.channel.ChannelHandlerContext;

/**
 * Authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public interface RestAuthenticator extends Closeable {

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

   /**
    * Returns whether the realm backing this authenticator is ready to authenticate users
    * @return a boolean indicating whether the real is empty (i.e. has no users)
    */
   default boolean isReadyForHttpChallenge() {
      return true;
   }

   @Override
   default void close() throws IOException {
      // No-op
   }
}
