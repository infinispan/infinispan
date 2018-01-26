package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
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
import org.infinispan.util.KeyValuePair;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;

/**
 * Handler that when added will make sure authentication is applied to requests.
 *
 * @author wburns
 * @since 9.0
 */
public class AuthenticationHandler extends ChannelInboundHandlerAdapter {
   private final static Log log = LogFactory.getLog(AuthenticationHandler.class, Log.class);

   private final HotRodServer server;

   private final HotRodServerConfiguration serverConfig;
   private final AuthenticationConfiguration authenticationConfig;
   private final boolean requireAuthentication;

   private SaslServer saslServer;
   private AuthorizingCallbackHandler callbackHandler;
   private Subject subject = ANONYMOUS;

   private final static Subject ANONYMOUS = new Subject();

   public AuthenticationHandler(HotRodServer server) {
      this.server = server;

      serverConfig = server.getConfiguration();
      authenticationConfig = server.getConfiguration().authentication();
         requireAuthentication = !authenticationConfig.mechProperties().containsKey(Sasl.POLICY_NOANONYMOUS)
            || authenticationConfig.mechProperties().get(Sasl.POLICY_NOANONYMOUS).equals("true");
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof CacheDecodeContext) {
         CacheDecodeContext cdc = (CacheDecodeContext) msg;
         HotRodHeader hrh = cdc.header;
         HotRodOperation op = hrh.op;
         switch (op) {
            case AUTH_MECH_LIST:
               writeResponse(ctx.channel(), new AuthMechListResponse(hrh.version, hrh.messageId, hrh.cacheName,
                     hrh.clientIntel, authenticationConfig.allowedMechs(), hrh.topologyId));
               break;
            // AUTH never requires authentication
            case AUTH:
               if (!serverConfig.authentication().enabled()) {
                  cdc.decoder.createErrorResponse(hrh, log.invalidOperation());
               } else {
                  // Retrieve the authorization context
                  KeyValuePair<String, byte[]> opContext = (KeyValuePair<String, byte[]>) cdc.operationDecodeContext;
                  if (saslServer == null) {
                     ServerAuthenticationProvider sap = authenticationConfig.serverAuthenticationProvider();
                     String mech = opContext.getKey();
                     callbackHandler = sap.getCallbackHandler(mech, authenticationConfig.mechProperties());
                     final SaslServerFactory ssf;
                     if ("EXTERNAL".equals(mech)) {
                        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
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
                  byte[] serverChallenge = saslServer.evaluateResponse(opContext.getValue());

                  writeResponse(ctx.channel(), new AuthResponse(hrh.version, hrh.messageId, hrh.cacheName,
                        hrh.clientIntel, serverChallenge, hrh.topologyId));
                  if (saslServer.isComplete()) {
                     List<Principal> extraPrincipals = new ArrayList<>();
                     String id = normalizeAuthorizationId(saslServer.getAuthorizationID());
                     extraPrincipals.add(new SimpleUserPrincipal(id));
                     extraPrincipals.add(new InetAddressPrincipal(((InetSocketAddress) ctx.channel().remoteAddress()).getAddress()));
                     SslHandler sslHandler = (SslHandler) ctx.pipeline().get("ssl");
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
                        ctx.pipeline().addBefore("decoder", "saslQop", qopHandler);
                     } else {
                        saslServer.dispose();
                        callbackHandler = null;
                        saslServer = null;
                     }
                  }
               }
               break;
            default:
               // We haven't yet authenticated don't let them run other commands unless the command is fine
               // not being authenticated
               if (requireAuthentication && op.requiresAuthentication() && subject == ANONYMOUS) {
                  throw log.unauthorizedOperation();
               }
               if (op.requiresAuthentication()) {
                  ((CacheDecodeContext)msg).withSubect(subject);
               }
               super.channelRead(ctx, msg);
               break;
         }
      } else {
         // We don't know the type, maybe throw an exception instead?
         super.channelRead(ctx, msg);
      }
   }

   String normalizeAuthorizationId(String id) {
      int realm = id.indexOf('@');
      if (realm >= 0) return id.substring(0, realm);
      else return id;
   }
}
