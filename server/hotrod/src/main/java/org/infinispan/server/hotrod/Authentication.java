package org.infinispan.server.hotrod;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.security.InetAddressPrincipal;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.SubjectSaslServer;
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
public class Authentication extends BaseRequestProcessor {
   private final static Log log = LogFactory.getLog(Authentication.class, Log.class);
   public static final String HOTROD_SASL_PROTOCOL = "hotrod";

   private final HotRodServerConfiguration serverConfig;
   private final AuthenticationConfiguration authenticationConfig;
   private final boolean enabled;
   private final boolean requireAuthentication;

   private SaslServer saslServer;
   private Subject subject = ANONYMOUS;

   private final static Subject ANONYMOUS = new Subject();

   public Authentication(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);

      serverConfig = server.getConfiguration();
      authenticationConfig = serverConfig.authentication();
      enabled = authenticationConfig.enabled();
      requireAuthentication = !authenticationConfig.mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
            || authenticationConfig.mechProperties().get(Sasl.POLICY_NOANONYMOUS).equals("true");
   }

   public void authMechList(HotRodHeader header) {
      writeResponse(header, header.encoder().authMechListResponse(header, server, channel.alloc(), authenticationConfig.allowedMechs()));
   }

   public void auth(HotRodHeader header, String mech, byte[] response) {
      if (!enabled) {
         UnsupportedOperationException cause = log.invalidOperation();
         ByteBuf buf = header.encoder().errorResponse(header, server, channel.alloc(), cause.toString(), OperationStatus.ServerError);
         int responseBytes = buf.readableBytes();
         ChannelFuture future = channel.writeAndFlush(buf);
         if (header instanceof AccessLoggingHeader) {
            server.accessLogging().logException(future, (AccessLoggingHeader) header, cause.toString(), responseBytes);
         }
      } else {
         executor.execute(() -> {
            try {
               authInternal(header, mech, response);
            } catch (Throwable t) {
               disposeSaslServer();
               writeException(header, t);
            }
         });
      }
   }

   private void authInternal(HotRodHeader header, String mech, byte[] response) throws Throwable {
      if (saslServer == null) {
         saslServer = createSaslServer(mech);
      }
      byte[] serverChallenge = saslServer.evaluateResponse(response);
      if (saslServer.isComplete()) {
         authComplete(header, serverChallenge);
      } else {
         // Write the server challenge
         writeResponse(header, header.encoder().authResponse(header, server, channel.alloc(), serverChallenge));
      }
   }

   private void authComplete(HotRodHeader header, byte[] serverChallenge) {
      // Obtaining the subject might be expensive, so do it before sending the final server challenge, otherwise the
      // client might send another operation before this is complete
      subject = (Subject) saslServer.getNegotiatedProperty(SubjectSaslServer.SUBJECT);
      String id = normalizeAuthorizationId(saslServer.getAuthorizationID());
      subject.getPrincipals().add(new SimpleUserPrincipal(id));
      // Finally we setup the QOP handler if required
      String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      if ("auth-int".equals(qop) || "auth-conf".equals(qop)) {
         channel.eventLoop().submit(() -> {
            writeResponse(header, header.encoder().authResponse(header, server, channel.alloc(), serverChallenge));
            SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
            channel.pipeline().addBefore("decoder", "saslQop", qopHandler);
         });
      } else {
         // Send the final server challenge
         writeResponse(header, header.encoder().authResponse(header, server, channel.alloc(), serverChallenge));
         disposeSaslServer();
         saslServer = null;
      }
   }

   private SaslServer createSaslServer(String mech) throws Throwable {
      ServerAuthenticationProvider sap = authenticationConfig.serverAuthenticationProvider();
      Set<Principal> principals = new HashSet<>();
      principals.add(new InetAddressPrincipal(((InetSocketAddress) channel.remoteAddress()).getAddress()));
      SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
      if (sslHandler != null) {
         try {
            principals.add(sslHandler.engine().getSession().getPeerPrincipal());
         } catch (SSLPeerUnverifiedException e) {
            if ("EXTERNAL".equals(mech)) {
               throw log.externalMechNotAllowedWithoutSSLClientCert();
            }
         }
      }
      if (authenticationConfig.serverSubject() != null) {
         try {
            return Subject.doAs(authenticationConfig.serverSubject(), (PrivilegedExceptionAction<SaslServer>) () ->
                  sap.createSaslServer(mech, principals, HOTROD_SASL_PROTOCOL, authenticationConfig.serverName(),
                        authenticationConfig.mechProperties()));
         } catch (PrivilegedActionException e) {
            throw e.getCause();
         }
      } else {
         return sap.createSaslServer(mech, principals, HOTROD_SASL_PROTOCOL, authenticationConfig.serverName(),
               authenticationConfig.mechProperties());
      }
   }

   private void disposeSaslServer() {
      try {
         if (saslServer != null)
            saslServer.dispose();
      } catch (SaslException e) {
         log.debug("Exception while disposing SaslServer", e);
      } finally {
         saslServer = null;
      }
   }

   public Subject getSubject(HotRodOperation operation) {
      if (!enabled || !operation.requiresAuthentication()) {
         return null;
      }

      // We haven't yet authenticated don't let them run other commands unless the command is fine
      // not being authenticated
      if (requireAuthentication && subject == ANONYMOUS) {
         throw log.unauthorizedOperation(operation.name());
      }
      return subject;
   }

   String normalizeAuthorizationId(String id) {
      int realm = id.indexOf('@');
      if (realm >= 0) return id.substring(0, realm);
      else return id;
   }
}
