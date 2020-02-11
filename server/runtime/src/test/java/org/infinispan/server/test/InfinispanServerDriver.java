package org.infinispan.server.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.management.MBeanServerConnection;
import javax.security.auth.x500.X500Principal;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.Server;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class InfinispanServerDriver {
   public static final String TEST_HOST_ADDRESS = "org.infinispan.test.host.address";
   public static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";

   protected final InfinispanServerTestConfiguration configuration;
   protected final InetAddress testHostAddress;

   private File rootDir;
   private File confDir;
   private ComponentStatus status;
   private AtomicLong certSerial = new AtomicLong(1);

   protected InfinispanServerDriver(InfinispanServerTestConfiguration configuration, InetAddress testHostAddress) {
      this.configuration = configuration;
      this.testHostAddress = testHostAddress;
      this.status = ComponentStatus.INSTANTIATED;
   }

   public ComponentStatus getStatus() {
      return status;
   }

   protected abstract void start(String name, File rootDir, String configurationFile);

   protected abstract void stop();

   /**
    * Prepare a server layout
    */
   public void prepare(String name) {
      String testDir = TestingUtil.tmpDirectory(name);
      Util.recursiveFileRemove(testDir);
      rootDir = new File(testDir);
      confDir = new File(rootDir, Server.DEFAULT_SERVER_CONFIG);
      if (!confDir.mkdirs()) {
         throw new RuntimeException("Failed to create server configuration directory " + confDir);
      }
      URL configurationFileURL;
      if (new File(configuration.configurationFile()).isAbsolute()) {
         configurationFileURL = Exceptions.unchecked(() -> new File(configuration.configurationFile()).toURI().toURL());
      } else {
         configurationFileURL = getClass().getClassLoader().getResource(configuration.configurationFile());
      }
      if (configurationFileURL == null) {
         throw new RuntimeException("Cannot find test configuration file: " + configuration.configurationFile());
      }
      Path configurationFilePath;
      try {
         configurationFilePath = Paths.get(configurationFileURL.toURI());
         // Recursively copy the contents of the directory containing the configuration file to the test target
         Util.recursiveDirectoryCopy(configurationFilePath.getParent(), confDir.toPath());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      createUserFile("default", true);
      createKeyStores();
   }

   public void start(String name) {
      InfinispanServerRule.log.infof("Starting server %s", name);
      start(name, rootDir, new File(configuration.configurationFile()).getName());
      InfinispanServerRule.log.infof("Started server %s", name);
      status = ComponentStatus.RUNNING;
   }

   public final void stop(String name) {
      status = ComponentStatus.STOPPING;
      InfinispanServerRule.log.infof("Stopping server %s", name);
      stop();
      InfinispanServerRule.log.infof("Stopped server %s", name);
      status = ComponentStatus.TERMINATED;
   }

   protected static File createServerHierarchy(File baseDir) {
      return createServerHierarchy(baseDir, null, null);
   }

   protected static File createServerHierarchy(File baseDir, String name) {
      return createServerHierarchy(baseDir, name, null);
   }

   protected static File createServerHierarchy(File baseDir, String name, BiConsumer<File, String> consumer) {
      File rootDir = name == null ? baseDir : new File(baseDir, name);
      for (String dir : Arrays.asList(
            Server.DEFAULT_SERVER_DATA,
            Server.DEFAULT_SERVER_LOG,
            Server.DEFAULT_SERVER_LIB)
      ) {
         File d = new File(rootDir, dir);
         if (!d.exists()) {
            if (!d.mkdirs()) {
               throw new IllegalStateException("Unable to create directory " + d);
            }
         }
         if (consumer != null) {
            consumer.accept(d, dir);
         }
      }
      return rootDir;
   }

   protected void createUserFile(String realm, boolean plain) {
      File userFile = new File(confDir, "users.properties");
      File groupsFile = new File(confDir, "groups.properties");
      try (PrintWriter uw = new PrintWriter(userFile); PrintWriter gw = new PrintWriter(groupsFile)) {
         uw.printf("#$REALM_NAME=%s$\n", realm);
         for (AuthorizationPermission permission : AuthorizationPermission.values()) {
            String name = permission.name().toLowerCase();
            String password;
            if (plain) {
               password = name;
            } else {
               try {
                  MessageDigest md = MessageDigest.getInstance("MD5");
                  password = Util.toHexString(md.digest(String.format("%s:%s:%s", name, realm, name).getBytes(StandardCharsets.UTF_8)));
               } catch (NoSuchAlgorithmException e) {
                  // will not happen, but acquiesce the compiler
                  throw new RuntimeException(e);
               }
            }

            uw.printf("%s_user=%s\n", name, password);
            gw.printf("%s_user=%s\n", name, name);
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public File getCertificateFile(String name) {
      return new File(confDir, name + ".pfx");
   }

   public File getConfDir() {
      return confDir;
   }

   /**
    * Creates a number of certificates in PKCS#12 format:
    * <ul>
    * <li><b>ca.pfx</b> A self-signed CA used as the main trust</li>
    * <li><b>server.pfx</b> A server certificate signed by the CA</li>
    * <li><b>user1.pfx</b> A client certificate signed by the CA</li>
    * <li><b>user2.pfx</b> A client certificate signed by the CA</li>
    * </ul>
    */
   protected void createKeyStores() {
      try {
         KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
         KeyPair keyPair = keyPairGenerator.generateKeyPair();
         PrivateKey signingKey = keyPair.getPrivate();
         PublicKey publicKey = keyPair.getPublic();

         X500Principal CA_DN = dn("CA");

         KeyStore trustStore = KeyStore.getInstance("pkcs12");
         trustStore.load(null);

         SelfSignedX509CertificateAndSigningKey ca = createSelfSignedCertificate(CA_DN, true, "ca");

         trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "server", trustStore);

         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "admin", trustStore);
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "supervisor", trustStore);
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "writer", trustStore);
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "reader", trustStore);

         try (FileOutputStream os = new FileOutputStream(getCertificateFile("trust"))) {
            trustStore.store(os, KEY_PASSWORD.toCharArray());
         }

         createSelfSignedCertificate(CA_DN, true, "untrusted");

      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

   protected SelfSignedX509CertificateAndSigningKey createSelfSignedCertificate(X500Principal dn, boolean isCA, String name) {
      SelfSignedX509CertificateAndSigningKey.Builder certificateBuilder = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(dn)
            .setKeyAlgorithmName("RSA")
            .setSignatureAlgorithmName("SHA1withRSA");
      if (isCA) {
         certificateBuilder.addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647");
      }
      SelfSignedX509CertificateAndSigningKey certificate = certificateBuilder.build();

      X509Certificate issuerCertificate = certificate.getSelfSignedCertificate();

      writeKeyStore(getCertificateFile(name), ks -> {
         try {
            ks.setCertificateEntry(name, issuerCertificate);
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      });
      return certificate;
   }

   protected void createSignedCertificate(PrivateKey signingKey, PublicKey publicKey,
                                          SelfSignedX509CertificateAndSigningKey ca,
                                          X500Principal issuerDN,
                                          String name, KeyStore trustStore) throws CertificateException {
      X509Certificate caCertificate = ca.getSelfSignedCertificate();
      X509Certificate certificate = new X509CertificateBuilder()
            .setIssuerDn(issuerDN)
            .setSubjectDn(dn(name))
            .setSignatureAlgorithmName("SHA1withRSA")
            .setSigningKey(ca.getSigningKey())
            .setPublicKey(publicKey)
            .setSerialNumber(BigInteger.valueOf(certSerial.getAndIncrement()))
            .addExtension(new BasicConstraintsExtension(false, false, -1))
            .build();

      try {
         trustStore.setCertificateEntry(name, certificate);
      } catch (KeyStoreException e) {
         throw new RuntimeException(e);
      }

      writeKeyStore(getCertificateFile(name), ks -> {
         try {
            ks.setCertificateEntry("ca", caCertificate);
            ks.setKeyEntry(name, signingKey, KEY_PASSWORD.toCharArray(), new X509Certificate[]{certificate, caCertificate});
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }

      });
   }

   private static void writeKeyStore(File file, Consumer<KeyStore> consumer) {
      try (FileOutputStream os = new FileOutputStream(file)) {
         KeyStore keyStore = KeyStore.getInstance("pkcs12");
         keyStore.load(null);
         consumer.accept(keyStore);
         keyStore.store(os, KEY_PASSWORD.toCharArray());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void applyKeyStore(ConfigurationBuilder builder, String certificateName) {
      builder.security().ssl().keyStoreFileName(getCertificateFile(certificateName).getAbsolutePath()).keyStorePassword(KEY_PASSWORD.toCharArray());
   }

   public void applyTrustStore(ConfigurationBuilder builder, String certificateName) {
      builder.security().ssl().trustStoreFileName(getCertificateFile(certificateName).getAbsolutePath()).trustStorePassword(KEY_PASSWORD.toCharArray());
   }

   public abstract boolean isRunning(int server);

   /**
    * Returns an InetSocketAddress for connecting to a specific port on a specific server. The implementation will need
    * to provide a specific mapping (e.g. port offset).
    *
    * @param server the index of the server
    * @param port   the service port
    * @return an unresolved InetSocketAddress pointing to the actual running service
    */
   public abstract InetSocketAddress getServerSocket(int server, int port);

   /**
    * Returns an InetAddress that points to a specific server.
    *
    * @param server the index of the server
    * @return an InetAddress pointing to the server's address
    */
   public abstract InetAddress getServerAddress(int server);

   /**
    * Pauses the server. Equivalent to kill -SIGSTOP
    *
    * @param server the index of the server
    */
   public void pause(int server) {
   }

   /**
    * Resumes a paused server. Equivalent to kill -SIGCONT
    *
    * @param server the index of the server
    */
   public abstract void resume(int server);

   /**
    * Gracefully stops a running server
    *
    * @param server the index of the server
    */
   public abstract void stop(int server);

   /**
    * Forcefully stops a server. Equivalent to kill -SIGKILL
    *
    * @param server the index of the server
    */
   public abstract void kill(int server);

   /**
    * Restarts a previously stopped server.
    *
    * @param server the index of the server
    */
   public abstract void restart(int server);

   /**
    * Restarts all of the nodes
    */
   public abstract void restartCluster();

   /**
    * Returns a {@link MBeanServerConnection} to the specified server
    *
    * @param server the index of the server
    */
   public abstract MBeanServerConnection getJmxConnection(int server);

   public abstract RemoteCacheManager createRemoteCacheManager(ConfigurationBuilder builder);

   /**
    * Access to container log
    *
    * @param server the index of the server
    * @return a String snapshot of a container's entire log output
    */
   public abstract String getLog(int server);

   /**
    * Returns the amount of time in seconds that we should wait for a server start/stop operation.
    * This may vary depending on the type of driver (embedded, container)
    *
    * @return the number of seconds after which a server start/stop is considered to timeout
    */
   public abstract int getTimeout();
}
