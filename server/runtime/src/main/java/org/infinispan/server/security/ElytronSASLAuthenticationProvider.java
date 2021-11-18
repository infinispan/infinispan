package org.infinispan.server.security;

import java.security.Principal;
import java.security.Provider;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.sasl.SaslAuthenticationFactory;
import org.wildfly.security.sasl.digest.WildFlyElytronSaslDigestProvider;
import org.wildfly.security.sasl.external.WildFlyElytronSaslExternalProvider;
import org.wildfly.security.sasl.gs2.WildFlyElytronSaslGs2Provider;
import org.wildfly.security.sasl.gssapi.WildFlyElytronSaslGssapiProvider;
import org.wildfly.security.sasl.localuser.WildFlyElytronSaslLocalUserProvider;
import org.wildfly.security.sasl.oauth2.WildFlyElytronSaslOAuth2Provider;
import org.wildfly.security.sasl.plain.WildFlyElytronSaslPlainProvider;
import org.wildfly.security.sasl.scram.WildFlyElytronSaslScramProvider;
import org.wildfly.security.sasl.util.AggregateSaslServerFactory;
import org.wildfly.security.sasl.util.FilterMechanismSaslServerFactory;
import org.wildfly.security.sasl.util.PropertiesSaslServerFactory;
import org.wildfly.security.sasl.util.ProtocolSaslServerFactory;
import org.wildfly.security.sasl.util.SecurityProviderSaslServerFactory;
import org.wildfly.security.sasl.util.ServerNameSaslServerFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronSASLAuthenticationProvider implements ServerAuthenticationProvider {
   private final SaslAuthenticationFactory saslAuthenticationFactory;

   public ElytronSASLAuthenticationProvider(String name, ServerSecurityRealm realm, String serverPrincipal, Collection<String> mechanisms) {
      Provider[] providers = new Provider[]{
            WildFlyElytronSaslPlainProvider.getInstance(),
            WildFlyElytronSaslDigestProvider.getInstance(),
            WildFlyElytronSaslScramProvider.getInstance(),
            WildFlyElytronSaslExternalProvider.getInstance(),
            WildFlyElytronSaslLocalUserProvider.getInstance(),
            WildFlyElytronSaslOAuth2Provider.getInstance(),
            WildFlyElytronSaslGssapiProvider.getInstance(),
            WildFlyElytronSaslGs2Provider.getInstance()
      };
      SecurityProviderSaslServerFactory securityProviderSaslServerFactory = new SecurityProviderSaslServerFactory(() -> providers);
      SaslAuthenticationFactory.Builder builder = SaslAuthenticationFactory.builder();
      AggregateSaslServerFactory factory = new AggregateSaslServerFactory(new FilterMechanismSaslServerFactory(securityProviderSaslServerFactory, true, mechanisms));
      builder.setFactory(factory);
      builder.setSecurityDomain(realm.getSecurityDomain());
      MechanismConfiguration.Builder mechConfigurationBuilder = MechanismConfiguration.builder();
      realm.applyServerCredentials(mechConfigurationBuilder, serverPrincipal);
      final MechanismRealmConfiguration.Builder mechRealmBuilder = MechanismRealmConfiguration.builder();
      mechRealmBuilder.setRealmName(name);
      mechConfigurationBuilder.addMechanismRealm(mechRealmBuilder.build());
      builder.setMechanismConfigurationSelector(MechanismConfigurationSelector.constantSelector(mechConfigurationBuilder.build()));
      saslAuthenticationFactory = builder.build();
   }

   @Override
   public SaslServer createSaslServer(String mechanism, List<Principal> principals, String protocol, String serverName, Map<String, String> props) throws SaslException {
      SaslServer saslServer = saslAuthenticationFactory.createMechanism(mechanism, factory -> {
         factory = new ServerNameSaslServerFactory(factory, serverName);
         factory = new ProtocolSaslServerFactory(factory, protocol);
         factory = props != null ? new PropertiesSaslServerFactory(factory, props) : factory;
         return factory;
      });
      return saslServer == null ? null : new ElytronSubjectSaslServer(saslServer, principals, null);
   }
}
