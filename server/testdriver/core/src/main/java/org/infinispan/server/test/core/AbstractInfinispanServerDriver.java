package org.infinispan.server.test.core;

import static org.wildfly.security.provider.util.ProviderUtil.findProvider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
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
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;

import org.infinispan.cli.user.UserTool;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.maven.Artifact;
import org.infinispan.commons.maven.MavenSettings;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.Server;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.test.api.TestUser;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Assume;
import org.wildfly.security.x500.GeneralName;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.SubjectAlternativeNamesExtension;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import net.spy.memcached.ConnectionFactoryBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class AbstractInfinispanServerDriver implements InfinispanServerDriver {
   public static final String DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME = "infinispan.xml";

   public static final String TEST_HOST_ADDRESS = "org.infinispan.test.host.address";
   public static final String JOIN_TIMEOUT = "jgroups.join_timeout";
   public static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final int JMX_PORT = 9999;

   protected final InfinispanServerTestConfiguration configuration;
   protected final InetAddress testHostAddress;
   private final SelfSignedX509CertificateAndSigningKey ca;

   private File rootDir;
   private File confDir;
   private ComponentStatus status;
   private final AtomicLong certSerial = new AtomicLong(1);
   private String name;

   private final Provider[] ALL_PROVIDERS;

   protected AbstractInfinispanServerDriver(InfinispanServerTestConfiguration configuration, InetAddress testHostAddress) {
      this.configuration = configuration;
      this.testHostAddress = testHostAddress;
      this.status = ComponentStatus.INSTANTIATED;
      ALL_PROVIDERS = SslContextFactory.discoverSecurityProviders(this.getClass().getClassLoader());
      ca = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(dn("CA"))
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setKeyAlgorithmName(KEY_ALGORITHM)
            .addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647")
            .build();
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

   protected String debugJvmOption() {
      String nonLoopbackAddress;
      try {
         nonLoopbackAddress = NetworkAddress.nonLoopback("").getAddress().getHostAddress();
      } catch (IOException e) {
         throw new IllegalStateException("Could not find a non-loopback address");
      }
      return String.format("-agentlib:jdwp=transport=dt_socket,server=n,address=%s:5005", nonLoopbackAddress);
   }

   protected abstract void stop();

   /**
    * Prepare a server layout
    */
   @Override
   public void prepare(String name) {
      this.name = name;
      if (configuration.getFeatures() != null) {
         // if the feature isn't enabled, the test will be skipped
         Features features = new Features(this.getClass().getClassLoader());
         for (String feature : configuration.getFeatures()) {
            Assume.assumeTrue(String.format("%s is disabled", feature), features.isAvailable(feature));
         }
      }

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
      createKeyStores(".pfx", "pkcs12", null);
      // Only create BCFKS certs if BouncyCastle is on the classpath
      if (findProvider(ALL_PROVIDERS, "BC", KeyStore.class, "BCFKS") != null) {
         createKeyStores(".bcfks", "BCFKS", "BC");
      }
   }

   @Override
   public void start(String name) {
      status = ComponentStatus.INITIALIZING;
      try {
         log.infof("Starting servers %s", name);
         start(name, rootDir, new File(configuration.configurationFile()));
         log.infof("Started servers %s", name);
         status = ComponentStatus.RUNNING;
      } catch (Throwable t) {
         log.errorf(t, "Unable to start server %s", name);
         status = ComponentStatus.FAILED;
         throw t;
      }
   }

   @Override
   public final void stop(String name) {
      if (status != ComponentStatus.INSTANTIATED) {
         status = ComponentStatus.STOPPING;
         log.infof("Stopping servers %s", name);
         stop();
         log.infof("Stopped servers %s", name);
      }
      status = ComponentStatus.TERMINATED;
   }

   private void copyProvidedServerConfigurationFile() {
      copyResource(configuration.configurationFile(), confDir.toPath());
   }

   private void copyResource(String resource, Path dst) {
      ClassLoader classLoader = getClass().getClassLoader();
      File configFile = new File(resource);
      if (configFile.isAbsolute()) {
         Path source = Paths.get(configFile.getParentFile().getAbsolutePath());
         Exceptions.unchecked(() -> Util.recursiveDirectoryCopy(source, dst));
         return;
      }

      URL resourceUrl = classLoader.getResource(resource);
      if (resourceUrl == null) {
         throw new RuntimeException("Cannot find test file: " + resource);
      }
      Exceptions.unchecked(() -> {
         if (resourceUrl.getProtocol().equals("jar")) {
            Map<String, String> env = new HashMap<>();
            env.put("create", "true");
            // If the resourceUrl is a path in a JAR, we must create a filesystem to avoid a FileSystemNotFoundException
            String[] parts = resourceUrl.toString().split("!");
            URI jarUri = new URI(parts[0]);
            try (FileSystem fs = FileSystems.newFileSystem(jarUri, env)) {
               String configJarPath = new File(parts[1]).getParentFile().toString();
               Path source = fs.getPath(configJarPath);
               Util.recursiveDirectoryCopy(source, dst);
            }
         } else {
            Path source = Paths.get(resourceUrl.toURI().resolve("."));
            Util.recursiveDirectoryCopy(source, dst);
         }
      });
   }

   protected static File createServerHierarchy(File baseDir) {
      return createServerHierarchy(baseDir, null);
   }

   protected static File createServerHierarchy(File baseDir, String name) {
      File rootDir = serverRoot(baseDir, name);
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
      }
      return rootDir;
   }

   protected static File serverRoot(File baseDir, String name) {
      return name == null ? baseDir : new File(baseDir, name);
   }

   protected void createUserFile(String realm) {
      // Create users and groups for individual permissions
      UserTool userTool = new UserTool(rootDir.getAbsolutePath());
      for (AuthorizationPermission permission : AuthorizationPermission.values()) {
         String name = permission.name().toLowerCase();
         userTool.createUser(name + "_user", name, realm, UserTool.Encryption.DEFAULT, Collections.singletonList(name), null);
      }
      // Create users with composite roles
      for (TestUser user : TestUser.values()) {
         if (user != TestUser.ANONYMOUS) {
            userTool.createUser(user.getUser(), user.getPassword(), realm, UserTool.Encryption.DEFAULT, user.getRoles(), null);
         }
      }
   }

   protected void copyArtifactsToDataDir() {
      if (configuration.getDataFiles() == null)
         return;

      File dataDir = new File(rootDir, Server.DEFAULT_SERVER_DATA);
      dataDir.mkdirs();
      for (String file : configuration.getDataFiles())
         copyResource(file, dataDir.toPath());
   }

   protected void copyArtifactsToUserLibDir(File libDir) {
      // Maven artifacts
      String propertyArtifacts = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EXTRA_LIBS);
      String[] artifacts = propertyArtifacts != null ? propertyArtifacts.replaceAll("\\s+", "").split(",") : configuration.mavenArtifacts();
      if (artifacts != null && artifacts.length > 0) {
         try {
            MavenSettings.init();
            for (String artifact : artifacts) {
               Path resolved = Artifact.fromString(artifact).resolveArtifact();
               Files.copy(resolved, libDir.toPath().resolve(resolved.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
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
      return new File(confDir, name);
   }

   @Override
   public File getRootDir() {
      return rootDir;
   }

   @Override
   public File getConfDir() {
      return confDir;
   }

   public String getName() {
      return name;
   }

   /**
    * Creates a number of certificates:
    * <ul>
    * <li><b>ca.pfx</b> A self-signed CA used as the main trust</li>
    * <li><b>server.pfx</b> A server certificate signed by the CA</li>
    * <li><b>user1.pfx</b> A client certificate signed by the CA</li>
    * <li><b>user2.pfx</b> A client certificate signed by the CA</li>
    * </ul>
    */
   protected void createKeyStores(String extension, String type, String providerName) {
      try {
         Provider provider = findProvider(ALL_PROVIDERS, providerName, KeyPairGenerator.class, KEY_ALGORITHM);
         KeyPairGenerator keyPairGenerator = provider != null ? KeyPairGenerator.getInstance(KEY_ALGORITHM, provider) : KeyPairGenerator.getInstance(KEY_ALGORITHM);
         KeyPair keyPair = keyPairGenerator.generateKeyPair();
         PrivateKey signingKey = keyPair.getPrivate();
         PublicKey publicKey = keyPair.getPublic();

         provider = findProvider(ALL_PROVIDERS, providerName, KeyStore.class, type);
         KeyStore trustStore = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
         trustStore.load(null, null);

         writeKeyStore(ca, "ca", extension, type, providerName);

         trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());
         createSignedCertificate(signingKey, publicKey, ca, "server", extension, trustStore, type, providerName);

         for (TestUser user : TestUser.values()) {
            if (user != TestUser.ANONYMOUS) {
               createSignedCertificate(signingKey, publicKey, ca, user.getUser(), extension, trustStore, type, providerName);
            }
         }
         createSignedCertificate(signingKey, publicKey, ca, "supervisor", extension, trustStore, type, providerName);

         try (FileOutputStream os = new FileOutputStream(getCertificateFile("trust" + extension))) {
            trustStore.store(os, KEY_PASSWORD.toCharArray());
         }

         SelfSignedX509CertificateAndSigningKey untrusted = SelfSignedX509CertificateAndSigningKey.builder()
               .setDn(dn("CA"))
               .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
               .setKeyAlgorithmName(KEY_ALGORITHM).addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647").build();
         writeKeyStore(untrusted, "untrusted", extension, type, providerName);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

   protected void writeKeyStore(SelfSignedX509CertificateAndSigningKey certificate, String name, String extension, String type, String providerName) {
      X509Certificate issuerCertificate = certificate.getSelfSignedCertificate();

      writeKeyStore(getCertificateFile(name + extension), type, providerName, ks -> {
         try {
            ks.setCertificateEntry(name, issuerCertificate);
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      });
      try (FileWriter w = new FileWriter(new File(confDir, name + extension + ".crt"))) {
         w.write("-----BEGIN CERTIFICATE-----\n");
         w.write(Base64.getEncoder().encodeToString(issuerCertificate.getEncoded()));
         w.write("\n-----END CERTIFICATE-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      try (FileWriter w = new FileWriter(new File(confDir, name + extension + ".key"))) {
         w.write("-----BEGIN PRIVATE KEY-----\n");
         w.write(Base64.getEncoder().encodeToString(certificate.getSigningKey().getEncoded()));
         w.write("\n-----END PRIVATE KEY-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   protected X509Certificate createSignedCertificate(PrivateKey signingKey, PublicKey publicKey,
                                                     SelfSignedX509CertificateAndSigningKey ca,
                                                     String name, String extension, KeyStore trustStore, String type, String providerName) throws CertificateException {
      X509Certificate caCertificate = ca.getSelfSignedCertificate();
      List<GeneralName> sANs = new ArrayList<>();
      sANs.add(new GeneralName.DNSName("infinispan.test"));
      sANs.add(new GeneralName.DNSName("localhost"));
      byte[] address = testHostAddress.getAddress();
      while (address[3] != -1) {
         try {
            sANs.add(new GeneralName.IPAddress(InetAddress.getByAddress(address).getHostAddress()));
         } catch (UnknownHostException e) {
            throw new RuntimeException(e);
         }
         address[3]++;
      }
      X509Certificate certificate = new X509CertificateBuilder()
            .setIssuerDn(ca.getSelfSignedCertificate().getSubjectX500Principal())
            .setSubjectDn(dn(name))
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setSigningKey(ca.getSigningKey())
            .setPublicKey(publicKey)
            .setSerialNumber(BigInteger.valueOf(certSerial.getAndIncrement()))
            .addExtension(new BasicConstraintsExtension(false, false, -1))
            .addExtension(new SubjectAlternativeNamesExtension(false, sANs))
            .build();

      if (trustStore != null) {
         try {
            trustStore.setCertificateEntry(name, certificate);
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      }

      writeKeyStore(getCertificateFile(name + extension), type, providerName, ks -> {
         try {
            ks.setCertificateEntry("ca", caCertificate);
            ks.setKeyEntry(name, signingKey, KEY_PASSWORD.toCharArray(), new X509Certificate[]{certificate, caCertificate});
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      });

      try (Writer w = Files.newBufferedWriter(getCertificateFile(name + ".pem").toPath())) {
         w.write("-----BEGIN PRIVATE KEY-----\n");
         w.write(Base64.getEncoder().encodeToString(ca.getSigningKey().getEncoded()));
         w.write("\n-----END PRIVATE KEY-----\n");
         w.write("-----BEGIN CERTIFICATE-----\n");
         w.write(Base64.getEncoder().encodeToString(certificate.getEncoded()));
         w.write("\n-----END CERTIFICATE-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      try (FileWriter w = new FileWriter(new File(confDir, name + extension + ".crt"))) {
         w.write("-----BEGIN CERTIFICATE-----\n");
         w.write(Base64.getEncoder().encodeToString(certificate.getEncoded()));
         w.write("\n-----END CERTIFICATE-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      try (FileWriter w = new FileWriter(new File(confDir, name + extension + ".key"))) {
         w.write("-----BEGIN PRIVATE KEY-----\n");
         w.write(Base64.getEncoder().encodeToString(ca.getSigningKey().getEncoded()));
         w.write("\n-----END PRIVATE KEY-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return certificate;
   }

   private void writeKeyStore(File file, String type, String providerName, Consumer<KeyStore> consumer) {
      try (FileOutputStream os = new FileOutputStream(file)) {
         Provider provider = findProvider(ALL_PROVIDERS, providerName, KeyStore.class, type);
         KeyStore keyStore = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
         keyStore.load(null, null);
         consumer.accept(keyStore);
         keyStore.store(os, KEY_PASSWORD.toCharArray());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void applyKeyStore(ConfigurationBuilder builder, String certificateName) {
      applyKeyStore(builder, certificateName, "pkcs12", null);
   }

   @Override
   public void applyKeyStore(ConfigurationBuilder builder, String certificateName, String type, String provider) {
      builder.security().ssl()
            .keyStoreFileName(getCertificateFile(certificateName).getAbsolutePath())
            .keyStorePassword(KEY_PASSWORD.toCharArray())
            .keyStoreType(type)
            .provider(provider);
   }

   @Override
   public void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName) {
      applyKeyStore(builder, certificateName, "pkcs12", null);
   }

   @Override
   public void applyKeyStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider) {
      builder.security().ssl()
            .keyStoreFileName(getCertificateFile(certificateName).getAbsolutePath())
            .keyStorePassword(KEY_PASSWORD.toCharArray())
            .keyStoreType(type)
            .provider(provider);
   }

   @Override
   public void applyTrustStore(ConfigurationBuilder builder, String certificateName) {
      applyTrustStore(builder, certificateName, "pkcs12", null);
   }

   @Override
   public void applyTrustStore(ConfigurationBuilder builder, String certificateName, String type, String provider) {
      builder.security().ssl()
            .trustStoreFileName(getCertificateFile(certificateName).getAbsolutePath())
            .trustStorePassword(KEY_PASSWORD.toCharArray())
            .trustStoreType(type)
            .provider(provider);
   }

   @Override
   public void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName) {
      applyTrustStore(builder, certificateName, "pkcs12", null);
   }

   @Override
   public void applyTrustStore(RestClientConfigurationBuilder builder, String certificateName, String type, String provider) {
      builder.security().ssl()
            .trustStoreFileName(getCertificateFile(certificateName).getAbsolutePath())
            .trustStorePassword(KEY_PASSWORD.toCharArray())
            .trustStoreType(type)
            .provider(provider);
   }

   @Override
   public void applyTrustStore(ConnectionFactoryBuilder builder, String certificateName) {
      applyTrustStore(builder, certificateName, "pkcs12", null);
   }

   @Override
   public void applyTrustStore(ConnectionFactoryBuilder builder, String certificateName, String type, String provider) {
      SslContextFactory factory = new SslContextFactory();
      SSLContext context = factory.trustStoreFileName(getCertificateFile(certificateName).getAbsolutePath())
            .trustStorePassword(KEY_PASSWORD.toCharArray())
            .trustStoreType(type)
            .provider(provider).build().sslContext();
      builder.setSSLContext(context).setSkipTlsHostnameVerification(true);
   }

   @Override
   public X509Certificate createCertificate(String name, String type, String providerName) {
      try {
         KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
         KeyPair keyPair = keyPairGenerator.generateKeyPair();
         return createSignedCertificate(keyPair.getPrivate(), keyPair.getPublic(), ca, name, ".pfx", null, type, providerName);
      } catch (CertificateException | NoSuchAlgorithmException e) {
         throw new RuntimeException(e);
      }
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

   public static String abbreviate(String name) {
      String[] split = name.split("\\.");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < split.length - 1; i++) {
         sb.append(split[i].charAt(0));
         sb.append('.');
      }
      sb.append(split[split.length - 1]);
      return sb.toString();
   }
}
