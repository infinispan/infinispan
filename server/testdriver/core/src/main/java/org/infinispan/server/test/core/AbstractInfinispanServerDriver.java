package org.infinispan.server.test.core;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.infinispan.cli.user.UserTool;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.maven.Artifact;
import org.infinispan.commons.maven.MavenSettings;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.NetworkAddress;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.testing.Exceptions;
import org.infinispan.testing.Testing;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.junit.Assume;

import net.spy.memcached.ConnectionFactoryBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class AbstractInfinispanServerDriver implements InfinispanServerDriver {
   public static final String DEFAULT_CLUSTERED_INFINISPAN_CONFIG_FILE_NAME = "infinispan.xml";

   public static final String TEST_HOST_ADDRESS = "org.infinispan.test.host.address";
   public static final String JOIN_TIMEOUT = "jgroups.join_timeout";
   public static final String KEY_PASSWORD = "secret";
   public static final int JMX_PORT = 9999;

   protected final InfinispanServerTestConfiguration configuration;
   protected final InetAddress testHostAddress;

   protected File rootDir;
   protected File confDir;
   protected ComponentStatus status;
   protected String name;

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

   @Override
   public InetAddress getTestHostAddress() {
      return testHostAddress;
   }

   protected abstract void start(String name, File rootDir);

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
      String testDir = Testing.tmpDirectory(siteName + name);
      Util.recursiveFileRemove(testDir);
      rootDir = new File(testDir);
      confDir = new File(rootDir, ServerConstants.DEFAULT_SERVER_CONFIG);
      if (!confDir.mkdirs()) {
         throw new RuntimeException("Failed to create server configuration directory " + confDir);
      }
      // if the file is not a default file, we need to copy the file from the resources folder to the server conf dir
      if (!configuration.isDefaultFile()) {
         copyProvidedServerConfigurationFile();
      }
      createUserFile("default");
      createKeyStores(CertificateAuthority.ExportType.PFX);
      if (CertificateAuthority.hasProvider("BC")) {
         createKeyStores(CertificateAuthority.ExportType.BCFKS);
      }
   }

   @Override
   public void start(String name) {
      status = ComponentStatus.INITIALIZING;
      try {
         log.infof("Starting servers %s", name);
         start(name, rootDir);
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
         ServerConstants.DEFAULT_SERVER_DATA,
         ServerConstants.DEFAULT_SERVER_LOG,
         ServerConstants.DEFAULT_SERVER_LIB)
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

      File dataDir = new File(rootDir, ServerConstants.DEFAULT_SERVER_DATA);
      dataDir.mkdirs();
      for (String file : configuration.getDataFiles())
         copyResource(file, dataDir.toPath());
   }

   protected void copyArtifactsToUserLibDir(File libDir) {
      // Maven artifacts
      String propertyArtifacts = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EXTRA_LIBS);
      if (propertyArtifacts != null) {
         addArtifactsToLibDir(libDir, propertyArtifacts.replaceAll("\\s+", "").split(","));
      }
      addArtifactsToLibDir(libDir, configuration.mavenArtifacts());

      // Supplied artifacts
      if (configuration.archives() != null) {
         for (Archive<?> artifact : configuration.archives()) {
            File jar = libDir.toPath().resolve(artifact.getName()).toFile();
            jar.setWritable(true, false);
            artifact.as(ZipExporter.class).exportTo(jar, true);
            log.infof("Artifact: %s", jar);
         }
      }
   }

   protected void addArtifactsToLibDir(File libDir, String... artifacts) {
      if (artifacts != null && artifacts.length > 0) {
         MavenSettings.init();
         for (String artifact : artifacts) {
            Artifact a = Artifact.fromString(artifact);
            log.infof("Artifact: %s", a);
            try {
               Path resolved = a.resolveArtifact();
               Files.copy(resolved, libDir.toPath().resolve(resolved.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
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
   protected void createKeyStores(CertificateAuthority.ExportType type) {
      try {
         CertificateAuthority certificateAuthority = configuration.certificateAuthority();
         certificateAuthority.exportCertificates(confDir.toPath().resolve("ca." + type.ext()), type, KEY_PASSWORD.toCharArray(), "ca");
         certificateAuthority.getCertificate("server", testHostAddress); // We want to add the IPAddress to the SAN
         certificateAuthority.exportCertificateWithKey("server", confDir.toPath(), KEY_PASSWORD.toCharArray(), type);
         for (TestUser user : TestUser.values()) {
            if (user != TestUser.ANONYMOUS) {
               certificateAuthority.exportCertificateWithKey(user.getUser(), confDir.toPath(), KEY_PASSWORD.toCharArray(), type);
            }
         }
         certificateAuthority.exportCertificateWithKey("supervisor", confDir.toPath(), KEY_PASSWORD.toCharArray(), type);
         certificateAuthority.exportCertificates(confDir.toPath().resolve("trust." + type.ext()), type, KEY_PASSWORD.toCharArray());
         certificateAuthority.exportUntrustedCertificate("untrusted", confDir.toPath(), KEY_PASSWORD.toCharArray(), type);
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
      String[] split = name.split("[./-]");
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < split.length - 1; i++) {
         if (split[i].isBlank())
            continue;
         sb.append(split[i].charAt(0));
         sb.append('.');
      }
      sb.append(split[split.length - 1]);
      return sb.toString();
   }
}
