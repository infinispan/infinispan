package org.infinispan.server.security;

import java.nio.file.Path;

import org.infinispan.server.test.core.KeyCloakServerExtension;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.utility.MountableFile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakSSLIT extends AbstractAuthenticationKeyCloak {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationKeyCloakSSLTest.xml")
               .build();

   @RegisterExtension
   public static KeyCloakServerExtension KEYCLOAK =
         new KeyCloakServerExtension(System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json"))
               .addBeforeListener(k -> {
                  Path serverConfPath = SERVERS.getServerDriver().getConfDir().toPath();
                  k.getKeycloakContainer()
                        .withCopyFileToContainer(MountableFile.forHostPath(serverConfPath.resolve("server.pfx")), "/opt/keycloak/conf/server.pfx")
                        .withCommand("start-dev", "--import-realm", "--https-key-store-file=/opt/keycloak/conf/server.pfx", "--https-key-store-password=secret");
               });

   public AuthenticationKeyCloakSSLIT() {
      super(SERVERS);
   }

   @Override
   protected String getToken() {
      return KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword", SERVERS.getServerDriver().getCertificateFile("ca.pfx").toPath(), "secret");
   }
}
