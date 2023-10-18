package org.infinispan.rest.authentication.impl;

import java.util.Optional;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;

import io.netty.channel.Channel;
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
         Channel parent = request.getRawContext().channel().parent();
         SslHandler sslHandler = parent != null ? parent.pipeline().get(SslHandler.class) : request.getRawContext().pipeline().get(SslHandler.class);
         SSLSession session = sslHandler.engine().getSession();
         Subject subject = new Subject();
         subject.getPrincipals().add(session.getPeerPrincipal());
         request.setSubject(subject);
         return;
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      throw new AuthenticationException(Optional.empty());
   }
}
