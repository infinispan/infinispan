package org.infinispan.server.hotrod;

import java.util.concurrent.Executor;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.configuration.SaslAuthenticationConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.SubjectSaslServer;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.core.transport.SaslQopHandler;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * Handler that when added will make sure authentication is applied to requests.
 *
 * @author wburns
 * @since 9.0
 */
public class Authentication extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(Authentication.class, Log.class);
   private static final Subject ANONYMOUS = new Subject();

   public static final String HOTROD_SASL_PROTOCOL = "hotrod";

   private final HotRodServerConfiguration serverConfig;
   private final SaslAuthenticationConfiguration authenticationConfig;
   private final boolean enabled;
   private final boolean requireAuthentication;

   private SaslServer saslServer;
   private Subject subject = ANONYMOUS;

   public Authentication(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);

      serverConfig = server.getConfiguration();
      authenticationConfig = serverConfig.authentication();
      enabled = authenticationConfig.enabled();
      requireAuthentication = !authenticationConfig.sasl().mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
            || "true".equals(authenticationConfig.sasl().mechProperties().get(Sasl.POLICY_NOANONYMOUS));
   }

   public void authMechList(HotRodHeader header) {
      // We have to make sure the default cache is initialized if we have one during an auth mech call
      if (server.hasDefaultCache()) {
         server.ensureCacheInitialized(header)
               .whenComplete((__, t) -> {
                  if (t != null) {
                     writeException(header, t);
                  } else {
                     writeResponse(header, header.encoder().authMechListResponse(header, server, channel,
                           authenticationConfig.sasl().mechanisms()));
                  }
               });
      } else {
         writeResponse(header, header.encoder().authMechListResponse(header, server, channel,
               authenticationConfig.sasl().mechanisms()));
      }
   }

   public void auth(HotRodHeader header, String mech, byte[] response) {
      if (!enabled) {
         UnsupportedOperationException cause = log.invalidOperation();
         ByteBuf buf = header.encoder().errorResponse(header, server, channel, cause.toString(), OperationStatus.ServerError);
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
               String message = t.getMessage();
               if (message.startsWith("ELY05055") || message.startsWith("ELY05051")) { // wrong credentials
                  writeException(header, log.authenticationException(t));
               } else {
                  writeException(header, t);
               }
            }
         });
      }
   }

   private void authInternal(HotRodHeader header, String mech, byte[] response) throws Throwable {
      if (saslServer == null) {
         saslServer = SaslAuthenticator.createSaslServer(serverConfig.authentication().sasl(), channel, mech, HOTROD_SASL_PROTOCOL);
         if (saslServer == null) {
            throw log.invalidMech(mech);
         }
      }
      byte[] serverChallenge = saslServer.evaluateResponse(response);
      if (saslServer.isComplete()) {
         authComplete(header, serverChallenge);
      } else {
         // Write the server challenge
         writeResponse(header, header.encoder().authResponse(header, server, channel, serverChallenge));
      }
   }

   private void authComplete(HotRodHeader header, byte[] serverChallenge) {
      // Obtaining the subject might be expensive, so do it before sending the final server challenge, otherwise the
      // client might send another operation before this is complete
      subject = (Subject) saslServer.getNegotiatedProperty(SubjectSaslServer.SUBJECT);
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(channel);
      metadata.subject(subject);
      // Finally we setup the QOP handler if required
      String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      if ("auth-int".equals(qop) || "auth-conf".equals(qop)) {
         channel.eventLoop().submit(() -> {
            writeResponse(header, header.encoder().authResponse(header, server, channel, serverChallenge));
            SaslQopHandler qopHandler = new SaslQopHandler(saslServer);
            channel.pipeline().addBefore("decoder", "saslQop", qopHandler);
         });
      } else {
         // Send the final server challenge
         writeResponse(header, header.encoder().authResponse(header, server, channel, serverChallenge));
         disposeSaslServer();
         saslServer = null;
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
}
