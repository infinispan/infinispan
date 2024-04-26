package org.infinispan.commons.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Instant;

import javax.security.auth.x500.X500Principal;

import org.infinispan.commons.io.FileWatcher;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Eventually;
import org.junit.Test;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;

/**
 * @since 15.0
 **/
public class SslContextFactoryTest {
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   public static final String SECRET = "secret";

   @Test
   public void testSslContextFactoryWatch() throws IOException {
      try (FileWatcher watcher = new FileWatcher()) {
         Path tmpDir = Paths.get(CommonsTestingUtil.tmpDirectory(SslContextFactoryTest.class));
         Files.createDirectories(tmpDir);
         Path keystore = createCertificateKeyStore("keystore", SECRET, tmpDir);
         Path truststore = createCertificateKeyStore("truststore", SECRET, tmpDir);
         SslContextFactory.Context context = new SslContextFactory()
               .keyStoreFileName(keystore.toString())
               .keyStorePassword(SECRET.toCharArray())
               .trustStoreFileName(truststore.toString())
               .trustStorePassword(SECRET.toCharArray())
               .watcher(watcher)
               .build();
         // Verify that building an SSLEngine works
         context.sslContext().createSSLEngine();

         // Recreate the keystore
         Instant kmLastLoaded = ((ReloadingX509KeyManager) context.keyManager()).lastLoaded();
         createCertificateKeyStore("keystore", SECRET, tmpDir);
         Eventually.eventually(() -> ((ReloadingX509KeyManager) context.keyManager()).lastLoaded().isAfter(kmLastLoaded));

         // Recreate the truststore
         Instant tmlastLoaded = ((ReloadingX509TrustManager) context.trustManager()).lastLoaded();
         createCertificateKeyStore("truststore", SECRET, tmpDir);
         Eventually.eventually(() -> ((ReloadingX509TrustManager) context.trustManager()).lastLoaded().isAfter(tmlastLoaded));

         // Verify that building an SSLEngine works
         context.sslContext().createSSLEngine();
      }
   }

   private Path createCertificateKeyStore(String name, String secret, Path dir) {
      SelfSignedX509CertificateAndSigningKey.Builder certificateBuilder = SelfSignedX509CertificateAndSigningKey.builder()
            .setDn(new X500Principal("CN=" + name))
            .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
            .setKeyAlgorithmName(KEY_ALGORITHM);
      SelfSignedX509CertificateAndSigningKey certificate = certificateBuilder.build();
      Path file = dir.resolve(name + ".pfx");
      try (OutputStream os = Files.newOutputStream(file)) {
         KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
         keyStore.load(null, null);
         keyStore.setCertificateEntry(name, certificate.getSelfSignedCertificate());
         keyStore.store(os, secret.toCharArray());
         return file;
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
