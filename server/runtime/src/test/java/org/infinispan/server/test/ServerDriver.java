package org.infinispan.server.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.function.Consumer;

import javax.security.auth.x500.X500Principal;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.Server;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class ServerDriver {
   private static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   private static final String KEY_PASSWORD = "secret";

   protected final String name;
   protected final ServerTestConfiguration configuration;

   protected ServerDriver(String name, ServerTestConfiguration configuration) {
      this.name = name;
      this.configuration = configuration;
   }

   protected abstract void before();

   protected abstract void after();


   protected static File createServerHierarchy(File baseDir, String name) {
      File rootDir = new File(baseDir, name);
      for (String dir : Arrays.asList(
            Server.DEFAULT_SERVER_DATA,
            Server.DEFAULT_SERVER_LOG,
            Server.DEFAULT_SERVER_LIB)
      ) {
         new File(rootDir, dir).mkdirs();
      }
      return rootDir;
   }

   protected static void createUserFile(File confDir, String prefix) {
      File userFile = new File(confDir, prefix + "-users.properties");
      File groupsFile = new File(confDir, prefix + "-groups.properties");
      try (PrintWriter uw = new PrintWriter(userFile); PrintWriter gw = new PrintWriter(groupsFile)) {
         for (AuthorizationPermission permission : AuthorizationPermission.values()) {
            String name = permission.name().toLowerCase();
            uw.printf("%s_user=%s$1\n", name, name);
            gw.printf("%s_user=%s\n", name, name);
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Creates a number of certificates in PKCS#12 format:
    * <ul>
    * <li><b>ca.pfx</b> A self-signed CA used as the main trust</li>
    * <li><b>server.pfx</b> A server certificate signed by the CA</li>
    * <li><b>user1.pfx</b> A client certificate signed by the CA</li>
    * <li><b>user2.pfx</b> A client certificate signed by the CA</li>
    * </ul>
    *
    * @param confDir
    */
   protected static void createKeyStores(File confDir) {
      try {
         KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
         KeyPair keyPair = keyPairGenerator.generateKeyPair();
         PrivateKey signingKey = keyPair.getPrivate();
         PublicKey publicKey = keyPair.getPublic();

         X500Principal CA_DN = dn("CA");

         SelfSignedX509CertificateAndSigningKey issuerSelfSignedX509CertificateAndSigningKey = SelfSignedX509CertificateAndSigningKey.builder()
               .setDn(CA_DN)
               .setKeyAlgorithmName("RSA")
               .setSignatureAlgorithmName("SHA1withRSA")
               .addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647")
               .build();
         X509Certificate issuerCertificate = issuerSelfSignedX509CertificateAndSigningKey.getSelfSignedCertificate();

         writeKeyStore(new File(confDir, "ca.pfx"), ks -> {
            try {
               ks.setCertificateEntry("ca", issuerCertificate);
            } catch (KeyStoreException e) {
               throw new RuntimeException(e);
            }
         });

         createCertificate(confDir, signingKey, publicKey, issuerSelfSignedX509CertificateAndSigningKey, issuerCertificate, CA_DN, "server");
         createCertificate(confDir, signingKey, publicKey, issuerSelfSignedX509CertificateAndSigningKey, issuerCertificate, CA_DN, "user1");
         createCertificate(confDir, signingKey, publicKey, issuerSelfSignedX509CertificateAndSigningKey, issuerCertificate, CA_DN, "user2");

      } catch (GeneralSecurityException e) {
         throw new RuntimeException(e);
      }
   }

   protected static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

   protected static void createCertificate(File confDir, PrivateKey signingKey, PublicKey publicKey,
                                         SelfSignedX509CertificateAndSigningKey issuerSelfSignedX509CertificateAndSigningKey,
                                         X509Certificate issuerCertificate, X500Principal issuerDN,
                                         String name) throws CertificateException {
      X509Certificate serverCertificate = new X509CertificateBuilder()
            .setIssuerDn(issuerDN)
            .setSubjectDn(dn(name))
            .setSignatureAlgorithmName("SHA1withRSA")
            .setSigningKey(issuerSelfSignedX509CertificateAndSigningKey.getSigningKey())
            .setPublicKey(publicKey)
            .setSerialNumber(new BigInteger("1"))
            .addExtension(new BasicConstraintsExtension(false, false, -1))
            .build();

      writeKeyStore(new File(confDir, name + ".pfx"), ks -> {
         try {
            ks.setCertificateEntry("ca", issuerCertificate);
            ks.setKeyEntry(name, signingKey, KEY_PASSWORD.toCharArray(), new X509Certificate[]{serverCertificate, issuerCertificate});
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

   public abstract InetSocketAddress getServerAddress(int server, int port);
}
