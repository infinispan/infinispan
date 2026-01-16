package org.infinispan.testing.security;

import java.io.OutputStream;
import java.io.Writer;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.security.auth.x500.X500Principal;

import org.wildfly.security.x500.GeneralName;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.SubjectAlternativeNamesExtension;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @since 15.0
 **/
public class TestCertificates {
   public static final String BASE_DN = "CN=%s,OU=Infinispan,O=JBoss,L=Red Hat";
   public static final char[] KEY_PASSWORD = "secret".toCharArray();
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";

   public static final String KEYSTORE_TYPE = KeyStore.getDefaultType();
   public static final String EXTENSION = ".pfx";

   private static final AtomicLong CERT_SERIAL = new AtomicLong(1);

   static {
      createKeyStores();
   }

   public static String certificate(String name) {
      return baseDir().resolve(name + EXTENSION).toString();
   }

   public static String pem(String name) {
      return baseDir().resolve(name + ".pem").toString();
   }

   private static void createKeyStores() {
      try {
         // Create the CA
         X500Principal CA_DN = dn("CA");
         SelfSignedX509CertificateAndSigningKey ca = createSelfSignedCertificate(CA_DN, "ca");

         KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
         KeyStore trustStore = KeyStore.getInstance(KEYSTORE_TYPE);
         trustStore.load(null, null);
         trustStore.setCertificateEntry("ca", ca.getSelfSignedCertificate());

         // Create a server certificate signed by the CA
         createSignedCertificate(ca, "server", trustStore, keyPairGenerator);

         // Create a client certificate signed by the CA
         createSignedCertificate(ca, "client", trustStore, keyPairGenerator);
         // A certificate for SNI tests
         createSignedCertificate(ca, "sni", trustStore, keyPairGenerator);

         // Write the trust store
         try (OutputStream os = Files.newOutputStream(getCertificateFile("trust" + EXTENSION))) {
            trustStore.store(os, KEY_PASSWORD);
         }

         // Create an untrusted certificate
         createSelfSignedCertificate(CA_DN, "untrusted");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static Path baseDir() {
      return Paths.get(System.getProperty("user.dir"), "target", "test-classes");
   }

   private static X500Principal dn(String cn) {
      return new X500Principal(String.format(BASE_DN, cn));
   }

   private static Path getCertificateFile(String name) {
      return baseDir().resolve(name);
   }

   private static SelfSignedX509CertificateAndSigningKey createSelfSignedCertificate(X500Principal dn, String name) {
      SelfSignedX509CertificateAndSigningKey certificate = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(dn)
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setKeyAlgorithmName(KEY_ALGORITHM)
            .addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647").build();
      X509Certificate issuerCertificate = certificate.getSelfSignedCertificate();
      writeKeyStore(getCertificateFile(name + EXTENSION), ks -> {
         try {
            ks.setCertificateEntry(name, issuerCertificate);
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }
      });
      try (Writer w = Files.newBufferedWriter(baseDir().resolve(name + ".pem"))) {
         w.write("-----BEGIN PRIVATE KEY-----\n");
         w.write(Base64.getEncoder().encodeToString(certificate.getSigningKey().getEncoded()));
         w.write("\n-----END PRIVATE KEY-----\n");
         w.write("-----BEGIN CERTIFICATE-----\n");
         w.write(Base64.getEncoder().encodeToString(issuerCertificate.getEncoded()));
         w.write("\n-----END CERTIFICATE-----\n");
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return certificate;
   }

   private static void createSignedCertificate(SelfSignedX509CertificateAndSigningKey ca,
                                               String name, KeyStore trustStore, KeyPairGenerator keyPairGenerator) throws CertificateException {
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      X509Certificate caCertificate = ca.getSelfSignedCertificate();
      X500Principal issuerDN = caCertificate.getIssuerX500Principal();
      X509Certificate certificate = new X509CertificateBuilder()
            .setIssuerDn(issuerDN)
            .setSubjectDn(dn(name))
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setSigningKey(ca.getSigningKey())
            .setPublicKey(keyPair.getPublic())
            .setSerialNumber(BigInteger.valueOf(CERT_SERIAL.getAndIncrement()))
            .addExtension(new BasicConstraintsExtension(false, false, -1))
            .addExtension(new SubjectAlternativeNamesExtension(false, List.of(new GeneralName.DNSName(name), new GeneralName.DNSName("localhost"))))
            .build();

      try {
         trustStore.setCertificateEntry(name, certificate);
      } catch (KeyStoreException e) {
         throw new RuntimeException(e);
      }

      writeKeyStore(getCertificateFile(name + EXTENSION), ks -> {
         try {
            ks.setCertificateEntry("ca", caCertificate);
            ks.setKeyEntry(name, keyPair.getPrivate(), KEY_PASSWORD, new X509Certificate[]{certificate, caCertificate});
         } catch (KeyStoreException e) {
            throw new RuntimeException(e);
         }

      });
   }

   private static void writeKeyStore(Path file, Consumer<KeyStore> consumer) {
      try (OutputStream os = Files.newOutputStream(file)) {
         KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
         keyStore.load(null, null);
         consumer.accept(keyStore);
         keyStore.store(os, KEY_PASSWORD);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
