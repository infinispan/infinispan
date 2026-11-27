package org.infinispan.server.test.core.ldap;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.apache.directory.ldap.client.api.LdapNetworkConnection;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A 389 Directory Server backed by testcontainers, using the official {@code docker.io/389ds/dirsrv} image.
 *
 * <p>The server is reachable on the host ports allocated per test (see {@code LdapServerListener}), exposing the
 * container's internal LDAP ({@value #LDAP_PORT}) and LDAPS ({@value #LDAPS_PORT}) ports. The container creates an
 * instance named {@code localhost} with a self-signed certificate for LDAPS. Unlike the other backends it does not
 * create any backend database itself; this class adds the standard test users by creating the {@code userRoot} backend
 * under {@value #DOMAIN} and importing the same {@code ldif/infinispan.ldif} used by {@link ApacheLdapServer}. The
 * import runs from this process over LDAP, since images prior to 3.0 do not ship an in-container <code>ldapadd</code>.
 *
 * <p>The 389 Directory Server hashes every written {@code userPassword}, so a realm backed by this server can only
 * verify credentials for which it holds password material (PLAIN, SCRAM); DIGEST mechanisms are not supported. Simple
 * binds over plain {@code ldap://} work out of the box. A different image can be substituted through the
 * {@value #IMAGE_PROPERTY} system property.
 */
public class Ds389LdapServer implements LdapServer {
   public static final String DS_389_IMAGE = "docker.io/389ds/dirsrv:3.1";
   public static final int LDAP_PORT = 3389;
   public static final int LDAPS_PORT = 3636;
   public static final String DOMAIN = "dc=infinispan,dc=org";
   public static final String ADMIN_DN = "cn=Directory Manager";
   public static final String ADMIN_PASSWORD = "strongPassword";

   /**
    * Overrides the default {@code 389ds/dirsrv:3.1} image with a prebuilt or version-pinned alternative.
    */
   public static final String IMAGE_PROPERTY = "org.infinispan.test.ds389.image";

   /** The same test users as {@link ApacheLdapServer}, shared via the classpath LDIF. */
   public static final String DEFAULT_LDIF = "ldif/infinispan.ldif";

   private static final Log log = LogFactory.getLog(Ds389LdapServer.class);
   private GenericContainer<?> container;

   @Override
   public void start(String keystoreFile, File confDir, LdapPorts ports) throws Exception {
      String image = System.getProperty(IMAGE_PROPERTY);
      if (image == null || image.isEmpty()) {
         image = DS_389_IMAGE;
      }
      log.infof("Using 389 Directory Server image '%s'", image);
      GenericContainer<?> server = new GenericContainer<>(image)
            .withEnv("DS_DM_PASSWORD", ADMIN_PASSWORD)
            .waitingFor(Wait.forLogMessage(".*INFO: 389-ds-container started\\..*", 1));
      server.setPortBindings(List.of(ports.ldap() + ":" + LDAP_PORT, ports.ldaps() + ":" + LDAPS_PORT));
      server.start();
      container = server;
      seedUsers(ports);
   }

   private void seedUsers(LdapPorts ports) throws Exception {
      // The 389ds/dirsrv image intentionally creates no result database, so add the standard userRoot first.
      ExecResult result = container.execInContainer("dsconf", "localhost", "backend", "create",
            "--suffix", DOMAIN, "--be-name", "userRoot", "--create-suffix");
      if (result.getExitCode() != 0) {
         throw new IllegalStateException("Failed to create userRoot result: stdout=[" + result.getStdout() +"], stderr=[" + result.getStderr() +"]");
      }
      // Images prior to 3.0 do not ship ldapadd, so add the users from this process over LDAP instead of exec'ing in the container.
      try (LdapNetworkConnection connection = new LdapNetworkConnection("localhost", ports.ldap());
           InputStream ldif = getClass().getClassLoader().getResourceAsStream(DEFAULT_LDIF)) {
         connection.bind(ADMIN_DN, ADMIN_PASSWORD);
         LdifImporter.importEntries(connection, ldif);
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
