package test.org.infinispan.spring.starter.remote;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.security.auth.x500.X500Principal;

import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;

public class CertUtil {
   public static void initCertificates(String keyStoreFileName, String trustStoreFileName, String alias) throws GeneralSecurityException, IOException {
      SelfSignedX509CertificateAndSigningKey cert = SelfSignedX509CertificateAndSigningKey.builder().setDn(new X500Principal("CN=test")).setKeyAlgorithmName("RSA").build();
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      keyStore.setKeyEntry(alias, cert.getSigningKey(), "secret".toCharArray(), new X509Certificate[]{cert.getSelfSignedCertificate()});
      try (OutputStream os = Files.newOutputStream(Paths.get(System.getProperty("build.directory"), "classes", keyStoreFileName), StandardOpenOption.CREATE)) {
         keyStore.store(os, "secret".toCharArray());
      }
      try (OutputStream os = Files.newOutputStream(Paths.get(System.getProperty("build.directory"), "classes", trustStoreFileName), StandardOpenOption.CREATE)) {
         keyStore.store(os, "secret".toCharArray());
      }
   }
}
