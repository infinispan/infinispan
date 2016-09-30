package org.infinispan.server.endpoint.subsystem;

import static java.util.Optional.ofNullable;
import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.infinispan.server.core.configuration.SslConfigurationBuilder;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.msc.service.StartException;
import org.jboss.msc.value.InjectedValue;

public class EncryptableServiceHelper {

   private EncryptableServiceHelper() {
   }

   public static void fillSecurityConfiguration(EncryptableService service, SslConfigurationBuilder configurationBuilder) throws StartException {
      if(isSecurityEnabled(service)) {
         SecurityRealm encryptionRealm = service.getEncryptionSecurityRealm().getValue();
         if (encryptionRealm != null) {
            SSLContext sslContext = encryptionRealm.getSSLContext();
            if (sslContext == null) {
               throw ROOT_LOGGER.noSSLContext(service.getServerName(), encryptionRealm.getName());
            }
            if (configurationBuilder.ssl().create().requireClientAuth() && !encryptionRealm.getSupportedAuthenticationMechanisms().contains(AuthMechanism.CLIENT_CERT)) {
               throw ROOT_LOGGER.noSSLTrustStore(service.getServerName(), encryptionRealm.getName());
            }
            configurationBuilder.ssl().enable();
            configurationBuilder.ssl().sslContext(sslContext);
            configurationBuilder.ssl().requireClientAuth(service.getClientAuth());
            for (Map.Entry<String, InjectedValue<SecurityRealm>> sniConfiguration : service.getSniConfiguration().entrySet()) {
               String sniDomain = sniConfiguration.getKey();
               SSLContext sniSslContext = Optional.ofNullable(sniConfiguration.getValue().getOptionalValue())
                     .flatMap(s -> ofNullable(s.getSSLContext()))
                     .orElseGet(() -> {
                        ROOT_LOGGER.noSSLContextForSni(service.getServerName());
                        return sslContext;
                     });
               configurationBuilder.ssl().sniHostName(sniDomain).sslContext(sniSslContext);
            }
         }
      }
   }

   public static boolean isSecurityEnabled(EncryptableService service) {
      return service.getEncryptionSecurityRealm().getOptionalValue() != null;
   }

   public static boolean isSniEnabled(EncryptableService service) {
      return isSecurityEnabled(service) && !service.getSniConfiguration().isEmpty();
   }

}
