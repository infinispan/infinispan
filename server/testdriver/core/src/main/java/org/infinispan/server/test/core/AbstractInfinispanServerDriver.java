package org.infinispan.server.test.core;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.security.auth.x500.X500Principal;

import org.infinispan.cli.user.UserTool;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolvedArtifact;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class AbstractInfinispanServerDriver implements InfinispanServerDriver {
   public static final String DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME = "infinispan.xml";

   public static final String TEST_HOST_ADDRESS = "org.infinispan.test.host.address";
   public static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";

   protected final InfinispanServerTestConfiguration configuration;
   protected final InetAddress testHostAddress;

   private File rootDir;
   private File confDir;
   private ComponentStatus status;
   private AtomicLong certSerial = new AtomicLong(1);

   protected AbstractInfinispanServerDriver(InfinispanServerTestConfiguration configuration, InetAddress testHostAddress) {
      this.configuration = configuration;
      this.testHostAddress = testHostAddress;
      this.status = ComponentStatus.INSTANTIATED;
   }

   @Override
   public ComponentStatus getStatus() {
      return status;
   }

   @Override
   public InfinispanServerTestConfiguration getConfiguration() {
      return configuration;
   }

   protected abstract void start(String name, File rootDir, File configurationFile);

   protected abstract void stop();

   /**
    * Prepare a server layout
    */
   @Override
   public void prepare(String name) {
      String siteName = configuration.site() == null ? "" : configuration.site();
      String testDir = CommonsTestingUtil.tmpDirectory(siteName + name);
      Util.recursiveFileRemove(testDir);
      rootDir = new File(testDir);
      confDir = new File(rootDir, Server.DEFAULT_SERVER_CONFIG);
      if (!confDir.mkdirs()) {
         throw new RuntimeException("Failed to create server configuration directory " + confDir);
      }
      // if the file is not a default file, we need to copy the file from the resources folder to the server conf dir
      if (!configuration.isDefaultFile()) {
         copyProvidedServerConfigurationFile();
      }
      createUserFile("default");
      createKeyStores();
   }

   @Override
   public void start(String name) {
      log.infof("Starting server %s", name);
      start(name, rootDir, new File(configuration.configurationFile()));
      log.infof("Started server %s", name);
      status = ComponentStatus.RUNNING;
   }

   @Override
   public final void stop(String name) {
      if (status == ComponentStatus.RUNNING) {
         status = ComponentStatus.STOPPING;
         log.infof("Stopping server %s", name);
         stop();
         log.infof("Stopped server %s", name);
      }
      status = ComponentStatus.TERMINATED;
   }

   private void copyProvidedServerConfigurationFile() {
      ClassLoader classLoader = getClass().getClassLoader();
      File configFile = new File(configuration.configurationFile());
      if (configFile.isAbsolute()) {
         Path source = Paths.get(configFile.getParentFile().getAbsolutePath());
         Exceptions.unchecked(() -> Util.recursiveDirectoryCopy(source, confDir.toPath()));
         return;
      }

      URL resourceUrl = classLoader.getResource(configuration.configurationFile());
      if (resourceUrl == null) {
         throw new RuntimeException("Cannot find test configuration file: " + configuration.configurationFile());
      }
      Exceptions.unchecked(() -> {
         if (resourceUrl.getProtocol().equals("jar")) {
            Map<String, String> env = new HashMap<>();
            env.put("create", "true");
            // If the resourceUrl is a path in a JAR, we must create a filesystem to avoid a FileSystemNotFoundException
            String[] parts = resourceUrl.toString().split("!");
            URI jarUri = new URI(parts[0]);
            try (FileSystem fs = FileSystems.newFileSystem(jarUri, env)) {
               String configJarPath  = new File(parts[1]).getParentFile().toString();
               Path source = fs.getPath(configJarPath);
               Util.recursiveDirectoryCopy(source, confDir.toPath());
            }
         } else {
            Path source = Paths.get(resourceUrl.toURI().resolve("."));
            Util.recursiveDirectoryCopy(source, confDir.toPath());
         }
      });
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

   protected void createUserFile(String realm) {
      // Create users and groups for individual permissions
      UserTool userTool = new UserTool(rootDir.getAbsolutePath());
      for (AuthorizationPermission permission : AuthorizationPermission.values()) {
         String name = permission.name().toLowerCase();
         userTool.createUser(name + "_user", name, realm, UserTool.Encryption.DEFAULT, Collections.singletonList(name), null);
      }
      // Create users with composite roles
      for(TestUser user : TestUser.values()) {
         userTool.createUser(user.getUser(), user.getPassword(), realm, UserTool.Encryption.DEFAULT, user.getRoles(), null);
      }
   }

   protected void copyArtifactsToUserLibDir(File libDir) {
      // Maven artifacts
      String propertyArtifacts = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EXTRA_LIBS);
      String[] artifacts = propertyArtifacts != null ? propertyArtifacts.replaceAll("\\s+", "").split(",") : configuration.mavenArtifacts();
      if (artifacts != null && artifacts.length > 0) {
         MavenResolvedArtifact[] archives = Maven.resolver().resolve(artifacts).withoutTransitivity().asResolvedArtifact();
         for (MavenResolvedArtifact archive : archives) {
            Exceptions.unchecked(() -> {
               Path source = archive.asFile().toPath();
               Files.copy(source, libDir.toPath().resolve(source.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            });
         }
      }
      // Supplied artifacts
      if (configuration.archives() != null) {
         for (JavaArchive artifact : configuration.archives()) {
            File jar = libDir.toPath().resolve(artifact.getName()).toFile();
            jar.setWritable(true, false);
            artifact.as(ZipExporter.class).exportTo(jar, true);
         }
      }
   }

   @Override
   public File getCertificateFile(String name) {
      return new File(confDir, name + ".pfx");
   }

   @Override
   public File getRootDir() {
      return rootDir;
   }

   @Override
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

   @Override
   public void applyKeyStore(ConfigurationBuilder builder, String certificateName) {
      builder.security().ssl().keyStoreFileName(getCertificateFile(certificateName).getAbsolutePath()).keyStorePassword(KEY_PASSWORD.toCharArray());
   }

   @Override
   public void applyTrustStore(ConfigurationBuilder builder, String certificateName) {
      builder.security().ssl().trustStoreFileName(getCertificateFile(certificateName).getAbsolutePath()).trustStorePassword(KEY_PASSWORD.toCharArray());
   }

   /**
    * Pauses the server. Equivalent to kill -SIGSTOP
    *
    * @param server the index of the server
    */
   @Override
   public void pause(int server) {
   }

   @Override
   public RemoteCacheManager createRemoteCacheManager(ConfigurationBuilder builder) {
      return new RemoteCacheManager(builder.build());
   }

}
