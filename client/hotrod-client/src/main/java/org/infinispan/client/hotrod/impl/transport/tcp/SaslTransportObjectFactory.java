package org.infinispan.client.hotrod.impl.transport.tcp;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.infinispan.client.hotrod.configuration.AuthenticationConfiguration;
import org.infinispan.client.hotrod.impl.operations.AuthMechListOperation;
import org.infinispan.client.hotrod.impl.operations.AuthOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * AuthenticatedTransportObjectFactory.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslTransportObjectFactory extends TransportObjectFactory {
   private static final Log log = LogFactory.getLog(SaslTransportObjectFactory.class);
   private static final byte[] EMPTY_BYTES = new byte[0];
   private static final String AUTH_INT = "auth-int";
   private static final String AUTO_CONF = "auth-conf";
   private final AuthenticationConfiguration configuration;

   public SaslTransportObjectFactory(Codec codec, TcpTransportFactory tcpTransportFactory, AtomicInteger topologyId,
         boolean pingOnStartup, AuthenticationConfiguration configuration) {
      super(codec, tcpTransportFactory, topologyId, pingOnStartup);
      this.configuration = configuration;
   }

   @Override
   public TcpTransport makeObject(SocketAddress address) throws Exception {
      TcpTransport tcpTransport = new TcpTransport(address, tcpTransportFactory);
      if (log.isTraceEnabled()) {
         log.tracef("Created tcp transport: %s", tcpTransport);
      }

      List<String> serverMechs = mechList(tcpTransport, topologyId);
      if (!serverMechs.contains(configuration.saslMechanism())) {
         throw log.unsupportedMech(configuration.saslMechanism(), serverMechs);
      }

      SaslClient saslClient;
      if (configuration.clientSubject() != null) {
         saslClient = Subject.doAs(configuration.clientSubject(), new PrivilegedExceptionAction<SaslClient>() {
            @Override
            public SaslClient run() throws Exception {
               CallbackHandler callbackHandler = configuration.callbackHandler();
               if (callbackHandler == null) {
                  callbackHandler = NoOpCallbackHandler.INSTANCE;
               }
            return Sasl.createSaslClient(new String[] { configuration.saslMechanism() }, null, "hotrod",
                     configuration.serverName(), configuration.saslProperties(), callbackHandler);
            }
         });
      } else {
         saslClient = Sasl.createSaslClient(new String[] { configuration.saslMechanism() }, null, "hotrod",
               configuration.serverName(), configuration.saslProperties(), configuration.callbackHandler());
      }

      if (log.isTraceEnabled()) {
         log.tracef("Authenticating using mech: %s", configuration.saslMechanism());
      }
      byte response[] = saslClient.hasInitialResponse() ? evaluateChallenge(saslClient, EMPTY_BYTES) : EMPTY_BYTES;

      byte challenge[] = auth(tcpTransport, topologyId, configuration.saslMechanism(), response);
      while (!saslClient.isComplete() && challenge != null) {
         response = evaluateChallenge(saslClient, challenge);
         if (response == null) {
            break;
         }
         challenge = auth(tcpTransport, topologyId, "", response);
      }

      /*String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
      if (qop != null && (qop.equalsIgnoreCase(AUTH_INT) || qop.equalsIgnoreCase(AUTO_CONF))) {
         tcpTransport.setSaslClient(saslClient);
      } else {*/
      saslClient.dispose();

      if (pingOnStartup && !firstPingExecuted) {
         log.trace("Executing first ping!");
         firstPingExecuted = true;

         // Don't ignore exceptions from ping() command, since
         // they indicate that the transport instance is invalid.
         ping(tcpTransport, topologyId);
      }
      return tcpTransport;
   }

   private byte[] evaluateChallenge(final SaslClient saslClient, final byte[] challenge) throws SaslException {
      if(configuration.clientSubject()!= null) {
         try {
            return Subject.doAs(configuration.clientSubject(), new PrivilegedExceptionAction<byte[]>() {
               @Override
               public byte[] run() throws Exception {
                  return saslClient.evaluateChallenge(challenge);
               }
            });
         } catch (PrivilegedActionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SaslException) {
               throw (SaslException)cause;
            } else {
               throw new RuntimeException(cause);
            }
         }
      } else {
         return saslClient.evaluateChallenge(challenge);
      }
   }

   private List<String> mechList(TcpTransport tcpTransport, AtomicInteger topologyId) {
      AuthMechListOperation op = new AuthMechListOperation(codec, topologyId, tcpTransport);
      return op.execute();
   }

   private byte[] auth(TcpTransport tcpTransport, AtomicInteger topologyId, String mech, byte[] response) {
      AuthOperation op = new AuthOperation(codec, topologyId, tcpTransport, mech, response);
      return op.execute();
   }

   public static final class NoOpCallbackHandler implements CallbackHandler {
      public static final NoOpCallbackHandler INSTANCE = new NoOpCallbackHandler();

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         // NO OP
      }
   }
}
