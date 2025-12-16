package org.infinispan.server.security;

import static org.infinispan.server.test.core.Containers.getContainerNetworkGateway;
import static org.infinispan.server.test.core.KeyCloakServerExtension.KEYCLOAK_HOSTNAME;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.server.test.core.CertificateAuthority;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.KeyCloakServerExtension;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.utility.MountableFile;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AuthenticationKeyCloakSSLIT extends AbstractAuthenticationKeyCloak {

   static final CertificateAuthority certificateAuthority = new CertificateAuthority();

   @RegisterExtension
   @Order(1)
   public static KeyCloakServerExtension KEYCLOAK =
      new KeyCloakServerExtension(
         System.getProperty(TestSystemPropertyNames.KEYCLOAK_REALM, "keycloak/infinispan-keycloak-realm.json")
      ).addBeforeListener(k -> {
         try {
            certificateAuthority.getCertificate("server", getContainerNetworkGateway(ContainerInfinispanServerDriver.NETWORK.getId()));
            Path serverCertificate = certificateAuthority.exportCertificateWithKey("server", Paths.get(CommonsTestingUtil.tmpDirectory(AuthenticationKeyCloakSSLIT.class.getName())), "secret".toCharArray(), CertificateAuthority.ExportType.PFX);
            k.getKeycloakContainer()
               .withCopyFileToContainer(MountableFile.forHostPath(serverCertificate), "/opt/keycloak/conf/server.pfx")
               .withCommand("start-dev", "--import-realm", "--hostname", KEYCLOAK_HOSTNAME, "--https-key-store-file=/opt/keycloak/conf/server.pfx", "--https-key-store-password=secret");
         } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
         }
      });

   @RegisterExtension
   @Order(2)
   public static final InfinispanServerExtension SERVERS =
      InfinispanServerExtensionBuilder.config("configuration/AuthenticationKeyCloakSSLTest.xml").certificateAuthority(certificateAuthority)
         .addListener(new KeyCloakServerExtension.KeyCloakServerAddressListener(KEYCLOAK))
         .build();

   public AuthenticationKeyCloakSSLIT() {
      super(SERVERS);
   }

   @Override
   protected String getToken() {
      return KEYCLOAK.getAccessTokenForCredentials(INFINISPAN_REALM, INFINISPAN_CLIENT_ID, INFINISPAN_CLIENT_SECRET, "admin", "adminPassword", SERVERS.getServerDriver().getCertificateFile("ca.pfx").toPath(), "secret");
   }
}
