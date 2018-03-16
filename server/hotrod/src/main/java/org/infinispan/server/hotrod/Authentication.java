package org.infinispan.server.hotrod;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.InetAddressPrincipal;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.external.ExternalSaslServerFactory;
import org.infinispan.server.core.security.simple.SimpleUserPrincipal;
import org.infinispan.server.core.transport.SaslQopHandler;
import org.infinispan.server.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslHandler;

/**
 * Handler that when added will make sure authentication is applied to requests.
 *
 * @author wburns
 * @since 9.0
 */
public class Authentication {
   private final static Log log = LogFactory.getLog(Authentication.class, Log.class);

   private final Channel channel;
   private final HotRodServer server;

   private final HotRodServerConfiguration serverConfig;
   private final AuthenticationConfiguration authenticationConfig;
   private final boolean enabled;
   private final boolean requireAuthentication;

   private SaslServer saslServer;
   private AuthorizingCallbackHandler callbackHandler;
   private Subject subject = ANONYMOUS;

   private final static Subject ANONYMOUS = new Subject();

   public Authentication(Channel channel, HotRodServer server) {
      this.channel = channel;
      this.server = server;

      serverConfig = server.getConfiguration();
      authenticationConfig = serverConfig.authentication();
      enabled = authenticationConfig.enabled();
      requireAuthentication = !authenticationConfig.mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
            || authenticationConfig.mechProperties().get(Sasl.POLICY_NOANONYMOUS).equals("true");
   }

   private void writeResponse(HotRodHeader header, ByteBuf response) {
      int responseBytes = response.readableBytes();
      ChannelFuture future = channel.writeAndFlush(response);
      if (header instanceof AccessLoggingHeader) {
         server.accessLogging().logOK(future, (AccessLoggingHeader) header, responseBytes);
      }
   }

   public void authMechList(HotRodHeader header) {
      writeResponse(header, header.encoder().authMechListResponse(header, server, channel.alloc(), authenticationConfig.allowedMechs()));
   }

   public void auth(HotRodHeader header, String mech, byte[] response) throws PrivilegedActionException, SaslException {
      if (!enabled) {
         UnsupportedOperationException cause = log.invalidOperation();
         ByteBuf buf = header.encoder().errorResponse(header, server, channel.alloc(), cause.toString(), OperationStatus.ServerError);
         int responseBytes = buf.readableBytes();
         ChannelFuture future = channel.writeAndFlush(buf);
         if (header instanceof AccessLoggingHeader) {
            server.accessLogging().logException(future, (AccessLoggingHeader) header, cause.toString(), responseBytes);
         }
      } else {
         if (saslServer == null) {
            ServerAuthenticationProvider sap = authenticationConfig.serverAuthenticationProvider();
            callbackHandler = sap.getCallbackHandler(mech, authenticationConfig.mechProperties());
            final SaslServerFactory ssf;
            if ("EXTERNAL".equals(mech)) {
               SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
               try {
                  if (sslHandler != null)
                     ssf = new ExternalSaslServerFactory(sslHandler.engine().getSession().getPeerPrincipal());
                  else
                     throw log.externalMechNotAllowedWithoutSSLClientCert();
               } catch (SSLPeerUnverifiedException e) {
                  throw log.externalMechNotAllowedWithoutSSLClientCert();
               }
            } else {
               ssf = server.getSaslServerFactory(mech);
            }
            if (authenticationConfig.serverSubject() != null) {
               saslServer = Subject.doAs(authenticationConfig.serverSubject(), (PrivilegedExceptionAction<SaslServer>) () ->
                     ssf.createSaslServer(mech, "hotrod", authenticationConfig.serverName(),
                           authenticationConfig.mechProperties(), callbackHandler));
            } else {
               saslServer = ssf.createSaslServer(mech, "hotrod", authenticationConfig.serverName(),
                     authenticationConfig.mechProperties(), callbackHandler);
            }
         }
         byte[] serverChallenge = saslServer.evaluateResponse(response);

         writeResponse(header, header.encoder().authResponse(header, server, channel.alloc(), serverChallenge));

         if (saslServer.isComplete()) {
            List<Principal> extraPrincipals = new ArrayList<>();
            String id = normalizeAuthorizationId(saslServer.getAuthorizationID());
            extraPrincipals.add(new SimpleUserPrincipal(id));
            extraPrincipals.add(new InetAddressPrincipal(((InetSocketAddress) channel.remoteAddress()).getAddress()));
            SslHandler sslHandler = (SslHandler) channel.pipeline().get("ssl");
            try {
               if (sslHandler != null)
                  extraPrincipals.add(sslHandler.engine().getSession().getPeerPrincipal());
            } catch (SSLPeerUnverifiedException e) {
               // Ignore any SSLPeerUnverifiedExceptions
            }
            subject = callbackHandler.getSubjectUserInfo(extraPrincipals).getSubject();
            String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
            if (qop != null && (qop.equalsIgnoreCase("auth-int") || qop.equalsIgnoreCase("auth-conf"))) {
               SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
               channel.pipeline().addBefore("decoder", "saslQop", qopHandler);
            } else {
               saslServer.dispose();
               callbackHandler = null;
               saslServer = null;
            }
         }
      }
   }

   public Subject getSubject(HotRodOperation op) {
      if (!enabled) {
         return null;
      }
      // We haven't yet authenticated don't let them run other commands unless the command is fine
      // not being authenticated
      if (requireAuthentication && op.requiresAuthentication() && subject == ANONYMOUS) {
         throw log.unauthorizedOperation();
      }
      if (op.requiresAuthentication()) {
         return subject;
      } else {
         return null;
      }
   }

   String normalizeAuthorizationId(String id) {
      int realm = id.indexOf('@');
      if (realm >= 0) return id.substring(0, realm);
      else return id;
   }
}
