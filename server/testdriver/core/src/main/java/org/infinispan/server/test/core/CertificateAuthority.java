package org.infinispan.server.test.core;

import static org.wildfly.security.provider.util.ProviderUtil.findProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.x500.X500Principal;

import org.infinispan.commons.util.SecurityProviders;
import org.wildfly.security.x500.GeneralName;
import org.wildfly.security.x500.cert.BasicConstraintsExtension;
import org.wildfly.security.x500.cert.SelfSignedX509CertificateAndSigningKey;
import org.wildfly.security.x500.cert.SubjectAlternativeNamesExtension;
import org.wildfly.security.x500.cert.X509CertificateBuilder;
import org.wildfly.security.x500.cert.X509CertificateChainAndSigningKey;

public class CertificateAuthority {

   public enum ExportType {
      PFX("pkcs12"),
      BCFKS("BC", "bcfks"),
      PEM("pem"),
      CRT("crt"),
      KEY("key");

      private final String providerName;
      private final String type;

      ExportType(String type) {
         this(null, type);
      }

      ExportType(String providerName, String type) {
         this.providerName = providerName;
         this.type = type;
      }

      public String ext() {
         return name().toLowerCase();
      }
   }

   public static final String DEFAULT_BASE_DN = "OU=server,DC=infinispan,DC=org";
   public static final String KEY_ALGORITHM = "RSA";
   public static final String KEY_SIGNATURE_ALGORITHM = "SHA256withRSA";
   private static final Provider[] ALL_PROVIDERS = SecurityProviders.discoverSecurityProviders(CertificateAuthority.class.getClassLoader());

   private final String baseDN;
   private final SelfSignedX509CertificateAndSigningKey ca;
   private final Map<String, X509CertificateChainAndSigningKey> certificates = new HashMap<>();
   protected final AtomicLong certSerial = new AtomicLong(1);
   private final KeyPairGenerator keyPairGenerator;

   public CertificateAuthority() {
      this(DEFAULT_BASE_DN);
   }

   public CertificateAuthority(String baseDN) {
      this.baseDN = baseDN;
      this.ca = SelfSignedX509CertificateAndSigningKey.builder()
         .setDn(dn("CA"))
         .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
         .setKeyAlgorithmName(KEY_ALGORITHM)
         .addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647")
         .build();
      certificates.put("ca", new X509CertificateChainAndSigningKey(new X509Certificate[]{ca.getSelfSignedCertificate()}, ca.getSigningKey()));
      Provider provider = findProvider(ALL_PROVIDERS, null, KeyPairGenerator.class, KEY_ALGORITHM);
      try {
         keyPairGenerator = provider != null ? KeyPairGenerator.getInstance(KEY_ALGORITHM, provider) : KeyPairGenerator.getInstance(KEY_ALGORITHM);
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException(e);
      }
   }

   public X509CertificateChainAndSigningKey getCertificate(String name) {
      return getCertificate(name, null);
   }

   public X509CertificateChainAndSigningKey getCertificate(String name, InetAddress host) {
      return certificates.computeIfAbsent(name, n -> {
         KeyPair keyPair = keyPairGenerator.generateKeyPair();
         PrivateKey signingKey = keyPair.getPrivate();
         PublicKey publicKey = keyPair.getPublic();
         X509Certificate caCertificate = ca.getSelfSignedCertificate();
         List<GeneralName> sANs = new ArrayList<>();
         sANs.add(new GeneralName.DNSName("infinispan.test"));
         sANs.add(new GeneralName.DNSName("localhost"));
         if (host != null) {
            byte[] address = host.getAddress();
            while (address[3] != -1) {
               try {
                  sANs.add(new GeneralName.IPAddress(InetAddress.getByAddress(address).getHostAddress()));
               } catch (UnknownHostException e) {
                  throw new RuntimeException(e);
               }
               address[3]++;
            }
         }
         try {
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
            return new X509CertificateChainAndSigningKey(new X509Certificate[]{certificate, caCertificate}, signingKey);
         } catch (CertificateException e) {
            throw new RuntimeException(e);
         }
      });
   }

   public Path exportCertificateWithKey(String name, Path toPath, char[] password, ExportType exportType) throws IOException, GeneralSecurityException {
      X509CertificateChainAndSigningKey certificate = getCertificate(name);
      Files.createDirectories(toPath);
      Path path = toPath.resolve(name + "." + exportType.ext());
      switch (exportType) {
         case PFX:
         case BCFKS: {
            KeyStore keyStore = getKeyStore(exportType.providerName, exportType.type);
            keyStore.setKeyEntry(name, certificate.getSigningKey(), password, certificate.getCertificateChain());
            try (OutputStream os = Files.newOutputStream(path)) {
               keyStore.store(os, password);
            }
            break;
         }
         case PEM: {
            try (Writer w = Files.newBufferedWriter(path)) {
               w.write("-----BEGIN PRIVATE KEY-----\n");
               w.write(Base64.getEncoder().encodeToString(certificate.getSigningKey().getEncoded()));
               w.write("\n-----END PRIVATE KEY-----\n");
               for (X509Certificate cert : certificate.getCertificateChain()) {
                  w.write("-----BEGIN CERTIFICATE-----\n");
                  w.write(Base64.getEncoder().encodeToString(cert.getEncoded()));
                  w.write("\n-----END CERTIFICATE-----\n");
               }
            }
            break;
         }
         case KEY: {
            try (Writer w = Files.newBufferedWriter(path)) {
               w.write("-----BEGIN PRIVATE KEY-----\n");
               w.write(Base64.getEncoder().encodeToString(certificate.getSigningKey().getEncoded()));
               w.write("\n-----END PRIVATE KEY-----\n");
            }
         }
         default: {
            throw new IllegalArgumentException(exportType.name());
         }
      }
      return path;
   }

   public void exportCertificates(Path path, ExportType exportType, char[] password, String... names) throws GeneralSecurityException, IOException {
      if (names == null || names.length == 0) {
         names = certificates.keySet().toArray(new String[0]);
      }
      Files.createDirectories(path.getParent());
      switch (exportType) {
         case PFX:
         case BCFKS: {
            KeyStore keyStore = getKeyStore(exportType.providerName, exportType.type);
            for (String name : names) {
               keyStore.setCertificateEntry(name, getCertificate(name).getCertificateChain()[0]);
            }
            try (OutputStream os = Files.newOutputStream(path)) {
               keyStore.store(os, password);
            }
            break;
         }
         default: {
            throw new IllegalArgumentException(exportType.name());
         }
      }
   }

   public void exportUntrustedCertificate(String name, Path path, char[] password, ExportType type) throws IOException, GeneralSecurityException {
      SelfSignedX509CertificateAndSigningKey certificate = SelfSignedX509CertificateAndSigningKey.builder()
         .setDn(dn(name))
         .setSignatureAlgorithmName(KEY_SIGNATURE_ALGORITHM)
         .setKeyAlgorithmName(KEY_ALGORITHM)
         .addExtension(false, "BasicConstraints", "CA:true,pathlen:2147483647")
         .build();
      KeyStore keyStore = getKeyStore(type.providerName, type.type);
      Files.createDirectories(path);
      keyStore.setKeyEntry(name, certificate.getSigningKey(), password, new X509Certificate[]{certificate.getSelfSignedCertificate()});
      try (OutputStream os = Files.newOutputStream(path.resolve(name + "." + type.ext()))) {
         keyStore.store(os, password);
      }
   }

   public static boolean hasProvider(String providerName) {
      for (Provider p : ALL_PROVIDERS) {
         if (p.getName().equals(providerName)) {
            return true;
         }
      }
      return false;
   }

   private static KeyStore getKeyStore(String providerName, String type) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
      Provider provider = findProvider(ALL_PROVIDERS, providerName, KeyStore.class, type);
      KeyStore keyStore = provider != null ? KeyStore.getInstance(type, provider) : KeyStore.getInstance(type);
      keyStore.load(null, null);
      return keyStore;
   }

   protected X500Principal dn(String cn) {
      return new X500Principal("cn=" + cn + "," + baseDN);
   }
}
