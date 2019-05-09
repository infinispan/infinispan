package org.infinispan.server.security;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.wildfly.security.auth.server.SaslAuthenticationFactory;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.sasl.util.AggregateSaslServerFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronAuthenticationProvider implements ServerAuthenticationProvider {
   private final SaslAuthenticationFactory saslAuthenticationFactory;

   public ElytronAuthenticationProvider(SecurityDomain domain) {
      SaslAuthenticationFactory.Builder builder = SaslAuthenticationFactory.builder();
      AggregateSaslServerFactory factory = new AggregateSaslServerFactory(SaslUtils.getSaslServerFactories());
      builder.setFactory(factory);
      builder.setSecurityDomain(domain);
      saslAuthenticationFactory = builder.build();
   }

   @Override
   public SaslServer createSaslServer(String mechanism, Set<Principal> principals, String protocol, String serverName, Map<String, String> props) throws SaslException {
      return saslAuthenticationFactory.createMechanism(mechanism);
   }
}
