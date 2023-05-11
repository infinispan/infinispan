package org.infinispan.server.security;

import java.security.Principal;
import java.security.Provider;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
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
public class ElytronSASLAuthenticator implements SaslAuthenticator {
   private final String name;
   private final String serverPrincipal;
   private final Collection<String> mechanisms;
   private SaslAuthenticationFactory saslAuthenticationFactory;

   public ElytronSASLAuthenticator(String name, String serverPrincipal, Collection<String> mechanisms) {
      this.name = name;
      this.serverPrincipal = serverPrincipal;
      this.mechanisms = mechanisms;
   }

   public static void init(HotRodServerConfiguration configuration, ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
      ElytronSASLAuthenticator authenticator = (ElytronSASLAuthenticator) configuration.authentication().sasl().authenticator();
      if (authenticator != null) {
         authenticator.init(serverConfiguration, timeoutExecutor);
      }
   }

   public static void init(MemcachedServerConfiguration configuration, ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
      ElytronSASLAuthenticator authenticator = (ElytronSASLAuthenticator) configuration.authentication().sasl().authenticator();
      if (authenticator != null) {
         authenticator.init(serverConfiguration, timeoutExecutor);
      }
   }

   public static void init(RespServerConfiguration configuration, ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
      RespAuthenticator authenticator = configuration.authentication().authenticator();
      if (authenticator != null) {
         ((ElytronRESPAuthenticator) authenticator).init(serverConfiguration, timeoutExecutor);
      }
   }

   public void init(ServerConfiguration serverConfiguration, ScheduledExecutorService timeoutExecutor) {
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
      ServerSecurityRealm realm = serverConfiguration.security().realms().getRealm(name).serverSecurityRealm();
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
