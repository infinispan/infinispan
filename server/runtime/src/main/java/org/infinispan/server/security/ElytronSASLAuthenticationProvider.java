package org.infinispan.server.security;

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.MechanismConfigurationSelector;
import org.wildfly.security.auth.server.MechanismRealmConfiguration;
import org.wildfly.security.auth.server.sasl.SaslAuthenticationFactory;
import org.wildfly.security.sasl.util.AggregateSaslServerFactory;
import org.wildfly.security.sasl.util.FilterMechanismSaslServerFactory;
import org.wildfly.security.sasl.util.PropertiesSaslServerFactory;
import org.wildfly.security.sasl.util.ProtocolSaslServerFactory;
import org.wildfly.security.sasl.util.SaslFactories;
import org.wildfly.security.sasl.util.SecurityProviderSaslServerFactory;
import org.wildfly.security.sasl.util.ServerNameSaslServerFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ElytronSASLAuthenticationProvider implements ServerAuthenticationProvider {
   private final String name;
   private final String serverPrincipal;
   private final Collection<String> mechanisms;
   private SaslAuthenticationFactory saslAuthenticationFactory;

   public ElytronSASLAuthenticationProvider(String name, String serverPrincipal, Collection<String> mechanisms) {
      this.name = name;
      this.serverPrincipal = serverPrincipal;
      this.mechanisms = mechanisms;
   }

   public static void init(HotRodServerConfiguration configuration, ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
      ElytronSASLAuthenticationProvider authenticator = (ElytronSASLAuthenticationProvider) configuration.authentication().serverAuthenticationProvider();
      if (authenticator != null) {
         authenticator.init(serverConfiguration, timeoutExecutor);
      }
   }

   public void init(ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
      ServerSecurityRealm realm = serverConfiguration.security().realms().getRealm(name).serverSecurityRealm();
      SaslAuthenticationFactory.Builder builder = SaslAuthenticationFactory.builder();
      SecurityProviderSaslServerFactory all = SaslFactories.getProviderSaslServerFactory();
      AggregateSaslServerFactory factory = new AggregateSaslServerFactory(new FilterMechanismSaslServerFactory(all, true, mechanisms));
      builder.setFactory(factory);
      builder.setSecurityDomain(realm.getSecurityDomain());
      MechanismConfiguration.Builder mechConfigurationBuilder = MechanismConfiguration.builder();
      realm.applyServerCredentials(mechConfigurationBuilder, serverPrincipal);
      final MechanismRealmConfiguration.Builder mechRealmBuilder = MechanismRealmConfiguration.builder();
      mechRealmBuilder.setRealmName(name);
      mechConfigurationBuilder.addMechanismRealm(mechRealmBuilder.build());
      builder.setMechanismConfigurationSelector(MechanismConfigurationSelector.constantSelector(mechConfigurationBuilder.build()));
      builder.setScheduledExecutorService(timeoutExecutor);
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
