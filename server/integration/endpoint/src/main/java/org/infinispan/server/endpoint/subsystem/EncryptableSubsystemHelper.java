package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.ExpressionResolver;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;

public class EncryptableSubsystemHelper {

   private EncryptableSubsystemHelper() {
   }

   static void processEncryption(ExpressionResolver context, ModelNode config, EncryptableService service, ServiceBuilder<?> builder) throws OperationFailedException {
      if (config.hasDefined(ModelKeys.ENCRYPTION) && config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME).isDefined()) {
         config = config.get(ModelKeys.ENCRYPTION, ModelKeys.ENCRYPTION_NAME);
         EndpointUtils.addSecurityRealmDependency(
               builder,
               EncryptionResource.SECURITY_REALM.resolveModelAttribute(context, config).asString(),
               service.getEncryptionSecurityRealm()
         );

         if(config.get(ModelKeys.SNI).isDefined()) {
            for(ModelNode sniConfiguration : config.get(ModelKeys.SNI).asList()) {
               // if the security realm is missing, a default one will be used
               ModelNode sni = sniConfiguration.get(0);
               if(sni.hasDefined(ModelKeys.SECURITY_REALM)) {
                  String sniHostName = SniResource.HOST_NAME.resolveModelAttribute(context, sni).asString();
                  String securityRealm = SniResource.SECURITY_REALM.resolveModelAttribute(context, sni).asString();
                  EndpointUtils.addSecurityRealmDependency(builder, securityRealm, service.getSniSecurityRealm(sniHostName));
               }
            }
         }
         service.setClientAuth(EncryptionResource.REQUIRE_SSL_CLIENT_AUTH.resolveModelAttribute(context, config).asBoolean());
      }
   }

}
