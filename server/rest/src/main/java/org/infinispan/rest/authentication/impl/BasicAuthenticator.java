package org.infinispan.rest.authentication.impl;

import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Collections;
import java.util.Optional;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.InetAddressPrincipal;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.SubjectUserInfo;

/**
 * BASIC authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class BasicAuthenticator implements Authenticator {

   private final ServerAuthenticationProvider serverAuthenticationProvider;
   private final String authenticateHeader;

   public BasicAuthenticator(ServerAuthenticationProvider serverAuthenticationProvider, String realm) {
      this.serverAuthenticationProvider = serverAuthenticationProvider;
      this.authenticateHeader = realm != null ? String.format("Basic realm=\"%s\"", realm) : "Basic";
   }

   @Override
   public void challenge(InfinispanRequest request) throws AuthenticationException {
      String auth = request.getAuthorization().orElseThrow(() -> new AuthenticationException(Optional.of(authenticateHeader)));
      if (auth.length() > 5) {
         String type = auth.substring(0, 5);
         type = type.toLowerCase();
         if ("basic".equals(type)) {
            String cookie = auth.substring(6);
            cookie = new String(Base64.getDecoder().decode(cookie.getBytes()));
            String[] split = cookie.split(":");
            String username = split[0];
            AuthorizingCallbackHandler handler = serverAuthenticationProvider.getCallbackHandler("PLAIN", Collections.emptyMap());

            NameCallback nameCallback = new NameCallback("User", username);
            PasswordCallback passwordCallback = new PasswordCallback("Password", false);
            AuthorizeCallback authorizeCallback = new AuthorizeCallback(username, username);
            passwordCallback.setPassword(split[1].toCharArray());
            try {
               handler.handle(new Callback[] { nameCallback, passwordCallback, authorizeCallback });
               InetSocketAddress remoteAddress = (InetSocketAddress) request.getRawContext().channel().remoteAddress();
               InetAddressPrincipal inetAddressPrincipal = new InetAddressPrincipal(remoteAddress.getAddress());
               SubjectUserInfo subjectUserInfo = handler.getSubjectUserInfo(Collections.singleton(inetAddressPrincipal));
               request.setSubject(subjectUserInfo.getSubject());
               return;
            } catch (Exception e) {
            }
         }
      }
      throw new AuthenticationException(Optional.of(authenticateHeader));
   }
}
