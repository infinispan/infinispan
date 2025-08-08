package org.infinispan.server.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.infinispan.server.test.core.KeyCloakServerExtension;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Tag("embedded")
public class AuthenticationKeyCloakIT extends AbstractAuthenticationKeyCloak {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationKeyCloakTest.xml")
               .build();

   @RegisterExtension
   public static KeyCloakServerExtension KEYCLOAK = new KeyCloakServerExtension(System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json"))
         .addBeforeListener(k -> {
            Path serverConfPath = SERVERS.getServerDriver().getConfDir().toPath();
            Path keyCloakPath = k.getKeycloakDirectory().toPath();
            try {
               Files.copy(serverConfPath.resolve("ca.pfx.crt"), keyCloakPath.resolve("tls.crt"), StandardCopyOption.REPLACE_EXISTING);
               Files.copy(serverConfPath.resolve("ca.pfx.key"), keyCloakPath.resolve("tls.key"), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         });

   public AuthenticationKeyCloakIT() {
      super(SERVERS);
   }

   @Override
   protected String getToken() {
      return KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword", null, null);
   }
}
