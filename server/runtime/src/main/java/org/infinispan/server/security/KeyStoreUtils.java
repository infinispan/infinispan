package org.infinispan.server.security;

import static org.wildfly.security.provider.util.ProviderUtil.findProvider;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.TimeZone;

import javax.security.auth.x500.X500Principal;

import org.wildfly.security.x500.cert.X509CertificateBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KeyStoreUtils {
   public static void generateSelfSignedCertificate(String keyStoreFileName, String provider, char[] keyStorePassword, char[] keyStoreCertificatePassword, String keyAlias, String host) throws IOException, GeneralSecurityException {
      KeyPairGenerator keyGen = provider != null ? KeyPairGenerator.getInstance("RSA", provider) : KeyPairGenerator.getInstance("RSA");
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

   public static void generateEmptyKeyStore(String keyStoreFileName, char[] keyStorePassword) throws IOException, GeneralSecurityException {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, keyStorePassword);
      try (FileOutputStream stream = new FileOutputStream(keyStoreFileName)) {
         keyStore.store(stream, keyStorePassword);
      }
   }

   public static KeyStore buildFilelessKeyStore(Provider[] providers, String providerName, String type) throws GeneralSecurityException, IOException {
      Provider provider = findProvider(providers, providerName, KeyStore.class, type);
      KeyStore keyStore = KeyStore.getInstance(type, provider);
      keyStore.load(null, null);
      return keyStore;
   }
}
