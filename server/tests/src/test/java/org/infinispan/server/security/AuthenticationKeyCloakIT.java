package org.infinispan.server.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.infinispan.server.test.core.KeyCloakServerRule;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakIT extends AbstractAuthenticationKeyCloak {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationKeyCloakTest.xml")
               .build();

   @ClassRule
   public static KeyCloakServerRule KEYCLOAK = new KeyCloakServerRule(System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json"))
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

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Override
   protected InfinispanServerTestMethodRule getServerTest() {
      return SERVER_TEST;
   }

   @Override
   protected String getToken() {
      return KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword", null, null);
   }
}
