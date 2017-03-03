package org.infinispan.rest.embedded.netty4.security;

import java.io.IOException;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.ws.rs.core.SecurityContext;

import org.infinispan.rest.embedded.netty4.NettySecurityContext;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponse;
import org.jboss.resteasy.util.HttpResponseCodes;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;

/**
 * Client Certificate authenticator
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class ClientCertAuthenticator implements Authenticator {

   private final SecurityDomain domain;

   public ClientCertAuthenticator(SecurityDomain domain) {
      this.domain = domain;
   }

   @Override
   public SecurityContext authenticate(ChannelHandlerContext ctx, HttpRequest request, HttpResponse response) throws IOException {
      SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
      try {
         SSLSession session = sslHandler.engine().getSession();
         return new NettySecurityContext(session.getPeerPrincipal(), domain, "ClientCert", true);
      } catch (SSLPeerUnverifiedException e) {
         // Ignore any SSLPeerUnverifiedExceptions
      }
      response.sendError(HttpResponseCodes.SC_UNAUTHORIZED);
      return null;
   }
}
