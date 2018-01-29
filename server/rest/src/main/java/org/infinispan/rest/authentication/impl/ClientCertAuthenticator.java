package org.infinispan.rest.authentication.impl;

import java.util.Optional;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;

import io.netty.handler.ssl.SslHandler;

/**
 * Client Certificate authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class ClientCertAuthenticator implements Authenticator {

   public ClientCertAuthenticator() {
   }

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {
      try {
         SslHandler sslHandler = request.getRawContext().pipeline().get(SslHandler.class);
         SSLSession session = sslHandler.engine().getSession();
         request.setPrincipal(session.getPeerPrincipal());
         return;
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      throw new AuthenticationException(Optional.empty());
   }
}
