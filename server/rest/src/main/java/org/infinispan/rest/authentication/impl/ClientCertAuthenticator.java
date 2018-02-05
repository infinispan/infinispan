package org.infinispan.rest.authentication.impl;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.callback.Callback;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.InetAddressPrincipal;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.SubjectUserInfo;

import io.netty.handler.ssl.SslHandler;

/**
 * Client Certificate authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class ClientCertAuthenticator implements Authenticator {
   private final ServerAuthenticationProvider serverAuthenticationProvider;

   public ClientCertAuthenticator(ServerAuthenticationProvider serverAuthenticationProvider) {
      this.serverAuthenticationProvider = serverAuthenticationProvider;
   }

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {
      try {
         AuthorizingCallbackHandler handler = serverAuthenticationProvider.getCallbackHandler("EXTERNAL", Collections.emptyMap());
         SslHandler sslHandler = request.getRawContext().pipeline().get(SslHandler.class);
         SSLSession session = sslHandler.engine().getSession();
         String name = session.getPeerPrincipal().getName();
         AuthorizeCallback authorizeCallback = new AuthorizeCallback(name, name);
         try {
            handler.handle(new Callback[] { authorizeCallback });
            List<Principal> extraPrincipals = new ArrayList<>(2);
            extraPrincipals.add(session.getPeerPrincipal());
            InetSocketAddress remoteAddress = (InetSocketAddress) request.getRawContext().channel().remoteAddress();
            extraPrincipals.add(new InetAddressPrincipal(remoteAddress.getAddress()));
            SubjectUserInfo subjectUserInfo = handler.getSubjectUserInfo(extraPrincipals);
            request.setSubject(subjectUserInfo.getSubject());
            return;
         } catch (Exception e) {
            throw new AuthenticationException(Optional.empty());
         }
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      throw new AuthenticationException(Optional.empty());
   }
}
