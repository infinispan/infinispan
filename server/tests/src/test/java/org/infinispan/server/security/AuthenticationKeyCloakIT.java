package org.infinispan.server.security;

import org.infinispan.server.test.core.KeyCloakServerExtension;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakIT extends AbstractAuthenticationKeyCloak {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationKeyCloakTest.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .build();

   @RegisterExtension
   public static KeyCloakServerExtension KEYCLOAK = new KeyCloakServerExtension(System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json"));

   public AuthenticationKeyCloakIT() {
      super(SERVERS);
   }

   @Override
   protected String getToken() {
      return KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword", null, null);
   }
}
