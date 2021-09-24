package org.infinispan.server.security;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Supplier;

import javax.security.auth.x500.X500Principal;

import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.pem.Pem;
import org.wildfly.security.pem.PemEntry;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KeyStoreUtils {
   public static void generateSelfSignedCertificate(String keyStoreFileName, String keyStoreType, char[] keyStorePassword, char[] keyStoreCertificatePassword, String keyAlias, String host) throws IOException, GeneralSecurityException {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyGen.initialize(2048, new SecureRandom());
      KeyPair pair = keyGen.generateKeyPair();

      PrivateKey privkey = pair.getPrivate();
      X509CertificateBuilder builder = new X509CertificateBuilder();
      Date from = new Date();
      Date to = new Date(from.getTime() + (1000L * 60L * 60L * 24L * 365L * 10L));
      BigInteger sn = new BigInteger(64, new SecureRandom());

      builder.setNotValidAfter(ZonedDateTime.ofInstant(Instant.ofEpochMilli(to.getTime()), TimeZone.getDefault().toZoneId()));
      builder.setNotValidBefore(ZonedDateTime.ofInstant(Instant.ofEpochMilli(from.getTime()), TimeZone.getDefault().toZoneId()));
      builder.setSerialNumber(sn);
      X500Principal owner = new X500Principal("CN=" + host);
      builder.setSubjectDn(owner);
      builder.setIssuerDn(owner);
      builder.setPublicKey(pair.getPublic());
      builder.setVersion(3);
      builder.setSignatureAlgorithmName("SHA256withRSA");
      builder.setSigningKey(privkey);

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, keyStorePassword);

      X509Certificate[] chain = new X509Certificate[1];
      chain[0] = builder.build();
      keyStore.setKeyEntry(keyAlias, pair.getPrivate(), keyStoreCertificatePassword != null ? keyStoreCertificatePassword : keyStorePassword, chain);
      try (FileOutputStream stream = new FileOutputStream(keyStoreFileName)) {
         keyStore.store(stream, keyStorePassword);
      }
   }

   public static KeyStore loadKeyStore(final Supplier<Provider[]> providers, final String providerName, FileInputStream is, String filename, char[] password) throws IOException, GeneralSecurityException {
      try {
         return KeyStoreUtil.loadKeyStore(providers, providerName, is, filename, password);
      } catch (KeyStoreException e) {
         return loadPemAsKeyStore(filename, password);
      }
   }

   public static KeyStore loadPemAsKeyStore(String filename, char[] password) throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null);
      // try to load it as a PEM
      PrivateKey pk = null;
      List<Certificate> certificates = new ArrayList<>();
      byte[] bytes = Files.readAllBytes(Paths.get(filename));
      for (Iterator<PemEntry<?>> it = Pem.parsePemContent(CodePointIterator.ofUtf8Bytes(bytes)); it.hasNext(); ) {
         Object entry = it.next().getEntry();
         if (entry instanceof PrivateKey) {
            // Private key
            pk = (PrivateKey) entry;
         } else if (entry instanceof Certificate) {
            // Certificate
            Certificate certificate = (Certificate) entry;
            certificates.add(certificate);
         }
      }
      if (pk != null) {
         // A keystore
         keyStore.setKeyEntry("key", pk, password, certificates.toArray(new Certificate[0]));
      } else {
         // A truststore
         int i = 1;
         for(Certificate certificate : certificates) {
            keyStore.setCertificateEntry(Integer.toString(i++), certificate);
         }
      }

      return keyStore;
   }
}
