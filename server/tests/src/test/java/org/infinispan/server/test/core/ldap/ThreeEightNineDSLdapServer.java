package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.util.List;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * A 389 Directory Server backed by testcontainers, using the official {@code docker.io/389ds/dirsrv} image.
 *
 * <p>The server is reachable on the same fixed ports as {@link ApacheLdapServer},
 * {@value #LDAP_HOST_PORT} (LDAP) and {@value #LDAPS_HOST_PORT} (LDAPS). The container creates an instance named
 * {@code localhost} listening on internal ports 3389/3636, with a self-signed certificate for LDAPS. Unlike the other
 * backends it does not create any backend database itself; this class adds the standard test users by creating the
 * {@code userRoot} backend under {@value #DOMAIN} and importing the same {@code ldif/infinispan.ldif} used by
 * {@link ApacheLdapServer}.
 *
 * <p>The 389 Directory Server hashes every written {@code userPassword}, so a realm backed by this server can only
 * verify credentials for which it holds password material (PLAIN, SCRAM); DIGEST mechanisms are not supported. Simple
 * binds over plain {@code ldap://} work out of the box. A different image can be substituted through the
 * {@value #IMAGE_PROPERTY} system property.
 */
public class ThreeEightNineDSLdapServer implements LdapServer {

   public static final int LDAP_PORT = 3389;
   public static final int LDAPS_PORT = 3636;
   public static final int LDAP_HOST_PORT = 10389;
   public static final int LDAPS_HOST_PORT = 10636;
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String ADMIN_DN = "cn=Directory Manager";
   public static final String ADMIN_PASSWORD = "strongPassword";

   /**
    * Overrides the default {@code 389ds/dirsrv:3.1} image with a prebuilt or version-pinned alternative.
    */
   public static final String IMAGE_PROPERTY = "org.infinispan.test.threeeightnineds.image";

   /** The same test users as {@link ApacheLdapServer}, shared via the classpath LDIF. */
   public static final String DEFAULT_LDIF = "ldif/infinispan.ldif";

   private static final Log log = LogFactory.getLog(ThreeEightNineDSLdapServer.class);
   private GenericContainer<?> container;

   @Override
   public void start(String keystoreFile, File confDir) throws Exception {
      String image = System.getProperty(IMAGE_PROPERTY);
      if (image == null || image.isEmpty()) {
         log.infof("Using 389 Directory Server image 'docker.io/389ds/dirsrv:3.1'");
         image = "docker.io/389ds/dirsrv:3.1";
      } else {
         log.infof("Using prebuilt 389 Directory Server image '%s'", image);
      }
      GenericContainer<?> server = new GenericContainer<>(image)
            .withEnv("DS_DM_PASSWORD", ADMIN_PASSWORD)
            .waitingFor(Wait.forLogMessage(".*INFO: 389-ds-container started\\..*", 1));
      server.setPortBindings(List.of(LDAP_HOST_PORT + ":" + LDAP_PORT, LDAPS_HOST_PORT + ":" + LDAPS_PORT));
      server.start();
      container = server;
      seedUsers();
   }

   private void seedUsers() throws Exception {
      // The 389ds/dirsrv image intentionally creates no backend database, so add the standard userRoot first.
      ExecResult backend = container.execInContainer("dsconf", "localhost", "backend", "create",
            "--suffix", DOMAIN, "--be-name", "userRoot", "--create-suffix");
      if (backend.getExitCode() != 0) {
         throw new IllegalStateException("Failed to create userRoot backend: " + backend.getStderr());
      }
      container.copyFileToContainer(MountableFile.forClasspathResource(DEFAULT_LDIF), "/tmp/users.ldif");
      ExecResult result = container.execInContainer("ldapadd", "-x", "-H", "ldap://localhost:" + LDAP_PORT,
            "-D", ADMIN_DN, "-w", ADMIN_PASSWORD, "-f", "/tmp/users.ldif");
      if (result.getExitCode() != 0) {
         throw new IllegalStateException("Failed to seed 389 Directory Server users: " + result.getStderr());
      }
      log.infof("Seeded 389 Directory Server users under %s", DOMAIN);
   }

   @Override
   public void stop() throws Exception {
      if (container != null) {
         container.stop();
         container = null;
      }
   }
}
