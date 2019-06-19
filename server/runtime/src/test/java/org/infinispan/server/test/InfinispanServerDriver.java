package org.infinispan.server.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
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
import java.util.function.Consumer;

import javax.security.auth.x500.X500Principal;

import org.infinispan.commons.util.Util;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.Server;
import org.infinispan.test.TestingUtil;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class InfinispanServerDriver {
   public static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   public static final String KEY_PASSWORD = "secret";

   protected final InfinispanServerTestConfiguration configuration;
   private File confDir;
   private ComponentStatus status;

   protected InfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      this.configuration = configuration;
      this.status = ComponentStatus.INSTANTIATED;
   }

   public ComponentStatus getStatus() {
      return status;
   }

   protected abstract void start(String name, File rootDir, String configurationFile);

   protected abstract void stop();

   public final void before(String name) {
      // Prepare a server layout
      String testDir = TestingUtil.tmpDirectory(name);
      Util.recursiveFileRemove(testDir);
      File rootDir = new File(testDir);
      confDir = new File(rootDir, Server.DEFAULT_SERVER_CONFIG);
      confDir.mkdirs();
      URL configurationFileURL = getClass().getClassLoader().getResource(configuration.configurationFile());
      if (configurationFileURL == null) {
         throw new RuntimeException("Cannot find test configuration file: "+ configuration.configurationFile());
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
      InfinispanServerRule.log.infof("Starting server %s", name);
      start(name, rootDir, configurationFilePath.getFileName().toString());
      InfinispanServerRule.log.infof("Started server %s", name);
      status = ComponentStatus.RUNNING;
   }

   public final void after(String name) {
      status = ComponentStatus.STOPPING;
      InfinispanServerRule.log.infof("Stopping server %s", name);
      stop();
      InfinispanServerRule.log.infof("Stopped server %s", name);
      status = ComponentStatus.TERMINATED;
   }

   protected static File createServerHierarchy(File baseDir, String name) {
      File rootDir = name == null ? baseDir : new File(baseDir, name);
      for (String dir : Arrays.asList(
            Server.DEFAULT_SERVER_DATA,
            Server.DEFAULT_SERVER_LOG,
            Server.DEFAULT_SERVER_LIB)
      ) {
         new File(rootDir, dir).mkdirs();
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

         SelfSignedX509CertificateAndSigningKey ca = createSelfSignedCertificate(CA_DN, true, "ca");

         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "server");
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "admin");
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "supervisor");
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "writer");
         createSignedCertificate(signingKey, publicKey, ca, CA_DN, "reader");

         createSelfSignedCertificate(CA_DN, true, "untrusted");

      } catch (GeneralSecurityException e) {
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
                                          String name) throws CertificateException {
      X509Certificate caCertificate = ca.getSelfSignedCertificate();
      X509Certificate serverCertificate = new X509CertificateBuilder()
            .setIssuerDn(issuerDN)
            .setSubjectDn(dn(name))
            .setSignatureAlgorithmName("SHA1withRSA")
            .setSigningKey(ca.getSigningKey())
            .setPublicKey(publicKey)
            .setSerialNumber(new BigInteger("1"))
            .addExtension(new BasicConstraintsExtension(false, false, -1))
            .build();

      writeKeyStore(getCertificateFile(name), ks -> {
         try {
            ks.setCertificateEntry("ca", caCertificate);
            ks.setKeyEntry(name, signingKey, KEY_PASSWORD.toCharArray(), new X509Certificate[]{serverCertificate, caCertificate});
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

   /**
    * Returns an InetSocketAddress for connecting to a specific port on a specific server. The implementation will
    * need to provide a specific mapping (e.g. port offset).
    *
    * @param server the index of the server
    * @param port the service port
    * @return an unresolved InetSocketeAddress pointing to the actual running service
    */
   public abstract InetSocketAddress getServerAddress(int server, int port);
}
