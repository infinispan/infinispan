package org.infinispan.server.endpoint.subsystem;

import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;

public class EncryptableSubsystemHelper {

   private EncryptableSubsystemHelper() {
   }

   static void processEncryption(ModelNode config, EncryptableService service, ServiceBuilder<?> builder) {
      if (config.hasDefined(ModelKeys.ENCRYPTION) && config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME).isDefined()) {
         EndpointUtils.addSecurityRealmDependency(builder, config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME, ModelKeys.SECURITY_REALM).asString(), service.getEncryptionSecurityRealm());
         config = config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME);
         if(config.get(ModelKeys.SNI).isDefined()) {
            for(ModelNode sniConfiguration : config.get(ModelKeys.SNI).asList()) {
               // if the security realm is missing, a default one will be used
               if(sniConfiguration.get(0).hasDefined(ModelKeys.SECURITY_REALM)) {
                  String sniHostName = sniConfiguration.get(0).get(ModelKeys.HOST_NAME).asString();
                  String securityRealm = sniConfiguration.get(0).get(ModelKeys.SECURITY_REALM).asString();
                  EndpointUtils.addSecurityRealmDependency(builder, securityRealm, service.getSniSecurityRealm(sniHostName));
               }
            }
         }
         if (config.hasDefined(ModelKeys.REQUIRE_SSL_CLIENT_AUTH)) {
            service.setClientAuth(config.get(ModelKeys.REQUIRE_SSL_CLIENT_AUTH).asBoolean());
         }
      }
   }

}
