package org.infinispan.server.core.security;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.x500.X500Principal;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.server.core.security.external.ExternalSaslServerFactory;


/**
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 * @author Tristan Tarrant
 */
public interface ServerAuthenticationProvider {

   /**
    * Get a callback handler for the given mechanism name.
    * <p>
    * This method is called each time a mechanism is selected for the connection and the resulting
    * AuthorizingCallbackHandler will be cached and used multiple times for this connection, AuthorizingCallbackHandler
    * should either be thread safe or the ServerAuthenticationProvider should provide a new instance each time called.
    *
    * @param mechanismName       the SASL mechanism to get a callback handler for
    * @param mechanismProperties the mechanism properties that might need to be adjusted to support the specific
    *                            mechanism / callbackhandler combination
    * @return the callback handler or {@code null} if the mechanism is not supported
    * @deprecated This method will no longer be invoked directly. Implement the
    * {@link #createSaslServer(String, Set, String, String, Map)} method instead.
    */
   @Deprecated
   default AuthorizingCallbackHandler getCallbackHandler(String mechanismName, Map<String, String> mechanismProperties) {
      return null;
   }

   /**
    * Create a SaslServer, to be used for a single authentication session, for the specified mechanismName. On
    * completion of the SASL authentication exchange, the SaslServer <b>MUST</b> provide a non-read-only negotiated
    * {@link javax.security.auth.Subject} when {@link SaslServer#getNegotiatedProperty(String)} is invoked with the
    * {@link SubjectSaslServer#SUBJECT} property. The default implementation of this method will
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
   default SaslServer createSaslServer(String mechanism, Set<Principal> principals, String protocol, String serverName, Map<String, String> props) throws SaslException {
      AuthorizingCallbackHandler callbackHandler = getCallbackHandler(mechanism, props);

      if ("EXTERNAL".equals(mechanism)) {
         // Find the X500Principal among the supplied principals
         for(Principal principal : principals) {
            if (principal instanceof X500Principal) {
               ExternalSaslServerFactory factory = new ExternalSaslServerFactory((X500Principal) principal);
               SaslServer saslServer = factory.createSaslServer(mechanism, protocol, serverName, props, callbacks -> {
                  if (callbackHandler != null) {
                     callbackHandler.handle(callbacks);
                  }
               });
               return new SubjectSaslServer(saslServer, principals, callbackHandler);
            }
         }
         throw new IllegalStateException("EXTERNAL mech requires X500Principal");
      } else {
         for (SaslServerFactory factory : SaslUtils.getSaslServerFactories()) {
            if (factory != null) {
               SaslServer saslServer = factory.createSaslServer(mechanism, protocol, serverName, props, callbacks -> {
                  if (callbackHandler != null) {
                     callbackHandler.handle(callbacks);
                  }
               });
               if (saslServer != null) {
                  return new SubjectSaslServer(saslServer, principals, callbackHandler);
               }
            }
         }
      }
      return null;
   }
}
