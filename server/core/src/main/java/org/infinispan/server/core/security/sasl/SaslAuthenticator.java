package org.infinispan.server.core.security.sasl;

import java.net.InetSocketAddress;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.server.core.configuration.SaslConfiguration;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.security.InetAddressPrincipal;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;


/**
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 * @author Tristan Tarrant
 */
public interface SaslAuthenticator {
   /**
    * Create a SaslServer, to be used for a single authentication session, for the specified mechanismName. On
    * completion of the SASL authentication exchange, the SaslServer <b>MUST</b> provide a non-read-only negotiated
    * {@link javax.security.auth.Subject} when {@link SaslServer#getNegotiatedProperty(String)} is invoked with the
    * {@link SubjectSaslServer#SUBJECT} property. The default implementation of this method wraps any matching {@link
    * SaslServerFactory} with a {@link SubjectSaslServer} to transparently supply the resolved Subject.
    *
    * @param mechanism  The non-null IANA-registered name of a SASL mechanism. (e.g. "GSSAPI", "CRAM-MD5").
    * @param principals Any principals which can be obtained before the authentication (e.g. TLS peer, remote network
    *                   address). Can be empty.
    * @param protocol   The non-null string name of the protocol for which the authentication is being performed (e.g.,
    *                   "ldap").
    * @param serverName The fully qualified host name of the server to authenticate to, or null if the server is not
    *                   bound to any specific host name. If the mechanism does not allow an unbound server, a {@code
    *                   SaslException} will be thrown.
    * @param props      The possibly null set of properties used to select the SASL mechanism and to configure the
    *                   authentication exchange of the selected mechanism. See the {@code Sasl} class for a list of
    *                   standard properties. Other, possibly mechanism-specific, properties can be included. Properties
    *                   not relevant to the selected mechanism are ignored, including any map entries with non-String
    *                   keys.
    * @return an instance of SaslServer (or null if it cannot be created)
    */
   default SaslServer createSaslServer(String mechanism, List<Principal> principals, String protocol, String serverName, Map<String, String> props) throws SaslException {
      throw new UnsupportedOperationException();
   }

   static SaslServer createSaslServer(SaslConfiguration configuration, Channel channel, String mech, String protocol) throws Throwable {
      SaslAuthenticator sap = configuration.authenticator();
      return createSaslServer(sap, configuration, channel, mech, protocol);
   }

   static SaslServer createSaslServer(SaslAuthenticator sap, SaslConfiguration configuration, Channel channel, String mech, String protocol) throws Throwable {
      List<Principal> principals = new ArrayList<>(2);
      SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
      if (sslHandler != null) {
         try {
            Principal peerPrincipal = sslHandler.engine().getSession().getPeerPrincipal();
            principals.add(peerPrincipal);
         } catch (SSLPeerUnverifiedException e) {
            if ("EXTERNAL".equals(mech)) {
               throw Log.SECURITY.externalMechNotAllowedWithoutSSLClientCert();
            }
         }
      }
      principals.add(new InetAddressPrincipal(((InetSocketAddress) channel.remoteAddress()).getAddress()));
      if (configuration != null && configuration.serverSubject() != null) {
         try {
            // We must use Subject.doAs() here instead of Security.doAs()
            return Subject.doAs(configuration.serverSubject(), (PrivilegedExceptionAction<SaslServer>) () ->
                  sap.createSaslServer(mech, principals, protocol, configuration.serverName(),
                        configuration.mechProperties()));
         } catch (PrivilegedActionException e) {
            throw e.getCause();
         }
      } else {
         Map<String, String> mechProperties = configuration != null ? configuration.mechProperties() : null;
         String serverName = configuration != null ? configuration.serverName() : null;
         return sap.createSaslServer(mech, principals, protocol, serverName, mechProperties);
      }
   }
}
