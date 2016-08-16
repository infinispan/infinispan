package org.infinispan.server.core.security.external;

import java.security.Principal;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

public final class ExternalSaslServerFactory implements SaslServerFactory {

   public static final String[] NAMES = new String[]{"EXTERNAL"};

   private final Principal peerPrincipal;

   public ExternalSaslServerFactory(final Principal peerPrincipal) {
      this.peerPrincipal = peerPrincipal;
   }

   public SaslServer createSaslServer(final String mechanism, final String protocol, final String serverName,
                                      final Map<String, ?> props, final CallbackHandler cbh) throws SaslException {
      return new ExternalSaslServer(cbh, peerPrincipal);
   }

   public String[] getMechanismNames(final Map<String, ?> props) {
      return NAMES;
   }
}