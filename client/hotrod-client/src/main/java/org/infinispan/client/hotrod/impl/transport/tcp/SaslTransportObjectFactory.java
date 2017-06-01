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
import org.infinispan.client.hotrod.configuration.Configuration;
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
   private static final boolean trace = log.isTraceEnabled();
   private static final byte[] EMPTY_BYTES = new byte[0];
   private static final String AUTH_INT = "auth-int";
   private static final String AUTH_CONF = "auth-conf";

   public SaslTransportObjectFactory(Codec codec, TcpTransportFactory tcpTransportFactory,
         AtomicInteger defaultCacheTopologyId, Configuration configuration) {
      super(codec, tcpTransportFactory, defaultCacheTopologyId, configuration);
   }

   @Override
   public TcpTransport makeObject(SocketAddress address) throws Exception {
      TcpTransport tcpTransport = new TcpTransport(address, tcpTransportFactory);
      if (trace) {
         log.tracef("Created tcp transport: %s", tcpTransport);
      }
      AuthenticationConfiguration authentication = configuration.security().authentication();

      List<String> serverMechs = mechList(tcpTransport, defaultCacheTopologyId);
      if (!serverMechs.contains(authentication.saslMechanism())) {
         throw log.unsupportedMech(authentication.saslMechanism(), serverMechs);
      }

      SaslClient saslClient;
      if (authentication.clientSubject() != null) {
         saslClient = Subject.doAs(authentication.clientSubject(), new PrivilegedExceptionAction<SaslClient>() {
            @Override
            public SaslClient run() throws Exception {
               CallbackHandler callbackHandler = authentication.callbackHandler();
               if (callbackHandler == null) {
                  callbackHandler = NoOpCallbackHandler.INSTANCE;
               }
            return Sasl.createSaslClient(new String[] { authentication.saslMechanism() }, null, "hotrod",
                  authentication.serverName(), authentication.saslProperties(), callbackHandler);
            }
         });
      } else {
         saslClient = Sasl.createSaslClient(new String[] { authentication.saslMechanism() }, null, "hotrod",
               authentication.serverName(), authentication.saslProperties(), authentication.callbackHandler());
      }

      if (trace) {
         log.tracef("Authenticating using mech: %s", authentication.saslMechanism());
      }
      byte response[] = saslClient.hasInitialResponse() ? evaluateChallenge(saslClient, EMPTY_BYTES, authentication.clientSubject()) : EMPTY_BYTES;

      byte challenge[] = auth(tcpTransport, defaultCacheTopologyId, authentication.saslMechanism(), response);
      while (!saslClient.isComplete() && challenge != null) {
         response = evaluateChallenge(saslClient, challenge, authentication.clientSubject());
         if (response == null) {
            break;
         }
         challenge = auth(tcpTransport, defaultCacheTopologyId, "", response);
      }

      String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
      if (qop != null && (qop.equalsIgnoreCase(AUTH_INT) || qop.equalsIgnoreCase(AUTH_CONF))) {
         tcpTransport.setSaslClient(saslClient);
      } else {
         saslClient.dispose();
      }

      if (!firstPingExecuted) {
         log.trace("Executing first ping!");
         firstPingExecuted = true;

         // Don't ignore exceptions from ping() command, since
         // they indicate that the transport instance is invalid.
         ping(tcpTransport, defaultCacheTopologyId);
      }
      return tcpTransport;
   }

   private byte[] evaluateChallenge(final SaslClient saslClient, final byte[] challenge, Subject clientSubject) throws SaslException {
      if(clientSubject != null) {
         try {
            return Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
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
      AuthMechListOperation op = new AuthMechListOperation(codec, topologyId, configuration, tcpTransport);
      return op.execute();
   }

   private byte[] auth(TcpTransport tcpTransport, AtomicInteger topologyId, String mech, byte[] response) {
      AuthOperation op = new AuthOperation(codec, topologyId, configuration, tcpTransport, mech, response);
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
