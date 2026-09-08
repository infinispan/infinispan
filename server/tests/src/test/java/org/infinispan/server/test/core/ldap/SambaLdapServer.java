package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

/**
 * A Samba 4 Active Directory Domain Controller backed by testcontainers.
 *
 * <p>By default, the image is built from the Dockerfile under
 * {@code src/test/resources/docker/samba-ad-dc}. A prebuilt image can be
 * substituted through the {@code org.infinispan.test.samba.image} system property.
 *
 * <p>The DC is reachable on the same fixed ports as {@link ApacheLdapServer},
 * {@value #LDAP_HOST_PORT} (LDAP) and {@value #LDAPS_HOST_PORT} (LDAPS). Unlike
 * Apache Directory, Samba stores entries as AD-style {@code CN=...} objects under
 * {@value #USERS_BASE_DN}; a Samba-backed realm therefore needs an AD-style bind
 * principal such as {@value #ADMIN_DN}. Simple binds over plain {@code ldap://} are
 * permitted by the container entrypoint; Samba otherwise requires strong auth.
 */
public class SambaLdapServer implements LdapServer {

   public static final int LDAP_PORT = 389;
   public static final int LDAPS_PORT = 636;
   public static final int LDAP_HOST_PORT = 10389;
   public static final int LDAPS_HOST_PORT = 10636;
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String REALM = "INFINISPAN.ORG";
   public static final String USERS_BASE_DN = "CN=Users," + DOMAIN;
   public static final String ADMIN_DN = "CN=Administrator," + USERS_BASE_DN;
   public static final String ADMIN_PASSWORD = "strongPassword@123";

   public static final String SAMBA_IMAGE_PROPERTY = "org.infinispan.test.samba.image";

   private static final Log log = LogFactory.getLog(SambaLdapServer.class);
   private static final String[][] USERS = {
         {"admin", "strongPassword"},
         {"deployer", "lessStrongPassword"},
         {"application", "somePassword"},
         {"observer", "password"},
         {"monitor", "weakPassword"},
         {"unprivileged", "weakPassword"},
         {"executor", "executorPassword"},
         {"reader", "readerPassword"},
         {"writer", "writerPassword"},
   };

   private GenericContainer<?> container;

   @Override
   public void start(String keystoreFile, File confDir) throws Exception {
      GenericContainer<?> server = new GenericContainer<>(image())
            .withExposedPorts(LDAP_PORT, LDAPS_PORT)
            .withPrivilegedMode(true)
            .withEnv("_REALM", REALM)
            .withEnv("_DOMAIN", "INFINISPAN")
            .withEnv("_PASSWORD", ADMIN_PASSWORD)
            .withEnv("_DNS_FORWARDER", "1.1.1.1 8.8.8.8")
            .waitingFor(Wait.forListeningPort());
      server.setPortBindings(List.of(LDAP_HOST_PORT + ":" + LDAP_PORT, LDAPS_HOST_PORT + ":" + LDAPS_PORT));
      server.start();
      container = server;
      seedUsers();
   }

   private static Future<String> image() throws Exception {
      String prebuilt = System.getProperty(SAMBA_IMAGE_PROPERTY);
      if (prebuilt != null && !prebuilt.isEmpty()) {
         log.infof("Using prebuilt Samba AD DC image '%s'", prebuilt);
         return new RemoteDockerImage(DockerImageName.parse(prebuilt));
      }
      URL dockerfile = Objects.requireNonNull(
            SambaLdapServer.class.getResource("/docker/samba-ad-dc/Dockerfile"),
            "samba-ad-dc Dockerfile not found on the classpath");
      Path dockerDir = Path.of(dockerfile.toURI()).toAbsolutePath().getParent();
      log.infof("Building Samba AD DC image from %s", dockerDir);
      return new ImageFromDockerfile("localhost/infinispan/samba-ad-dc:latest", true)
            .withFileFromPath("Dockerfile", dockerDir.resolve("Dockerfile"))
            .withFileFromPath("entrypoint.sh", dockerDir.resolve("entrypoint.sh"));
   }

   private void seedUsers() throws Exception {
      StringBuilder script = new StringBuilder();
      script.append("for i in $(seq 1 120); do samba-tool user list >/dev/null 2>&1 && break; sleep 1; done\n");
      for (String[] user : USERS) {
         script.append("samba-tool user create ").append(user[0]).append(" ").append(user[1]).append(" || true\n");
      }
      ExecResult result = container.execInContainer("bash", "-c", script.toString());
      if (result.getExitCode() != 0) {
         throw new IllegalStateException("Failed to seed Samba users: " + result.getStderr());
      }
      log.infof("Seeded Samba users:\n%s", result.getStdout().trim());
   }

   @Override
   public void stop() throws Exception {
      if (container != null) {
         container.stop();
         container = null;
      }
   }
}
