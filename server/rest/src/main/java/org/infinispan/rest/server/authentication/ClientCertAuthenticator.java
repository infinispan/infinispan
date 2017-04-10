package org.infinispan.rest.server.authentication;

import java.util.Optional;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.infinispan.rest.server.InfinispanRequest;
import org.infinispan.rest.server.RestResponseException;
import org.infinispan.rest.server.authentication.exceptions.AuthenticationException;

import io.netty.handler.ssl.SslHandler;

public class ClientCertAuthenticator implements Authenticator {

   public ClientCertAuthenticator() {
   }

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {
      try {
         SslHandler sslHandler = request.getRawContext().pipeline().get(SslHandler.class);
         SSLSession session = sslHandler.engine().getSession();
         session.getPeerPrincipal();
         return;
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      throw new AuthenticationException(Optional.empty());
   }
}
