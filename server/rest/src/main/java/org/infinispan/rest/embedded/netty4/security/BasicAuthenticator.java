package org.infinispan.rest.embedded.netty4.security;

import java.io.IOException;
import java.security.Principal;
import java.util.Base64;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

import org.infinispan.rest.embedded.netty4.NettySecurityContext;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.util.HttpHeaderNames;
import org.jboss.resteasy.util.HttpResponseCodes;

import io.netty.channel.ChannelHandlerContext;

/**
 * An Authenticator which implements the Basic authentication method
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class BasicAuthenticator implements Authenticator {

   private final SecurityDomain domain;
   private final boolean secure;
   private final String realm;
   private final String authenticateHeader;

   public BasicAuthenticator(SecurityDomain domain, boolean secure, String realm) {
      this.domain = domain;
      this.secure = secure;
      this.realm = realm;
      this.authenticateHeader = realm != null ? String.format("Basic realm=\"%s\"", realm) : "Basic";
   }

   @Override
   public SecurityContext authenticate(ChannelHandlerContext ctx, HttpRequest request, HttpResponse response) throws IOException {
      List<String> headers = request.getHttpHeaders().getRequestHeader(HttpHeaderNames.AUTHORIZATION);
      if (!headers.isEmpty()) {
         String auth = headers.get(0);
         if (auth.length() > 5) {
            String type = auth.substring(0, 5);
            type = type.toLowerCase();
            if ("basic".equals(type)) {
               String cookie = auth.substring(6);
               cookie = new String(Base64.getDecoder().decode(cookie.getBytes()));
               String[] split = cookie.split(":");
               try {
                  Principal user = domain.authenticate(split[0], split[1]);
                  return new NettySecurityContext(user, domain, "BASIC", secure);
               } catch (SecurityException e) {
                  sendUnauthorizedResponse(response);
                  return null;
               }
            } else {
               sendUnauthorizedResponse(response);
               return null;
            }
         }
      }
      sendUnauthorizedResponse(response);
      return null;
   }

   private void sendUnauthorizedResponse(HttpResponse response) throws IOException {
      response.getOutputHeaders().add(HttpHeaderNames.WWW_AUTHENTICATE, authenticateHeader);
      response.sendError(HttpResponseCodes.SC_UNAUTHORIZED);
   }
}
