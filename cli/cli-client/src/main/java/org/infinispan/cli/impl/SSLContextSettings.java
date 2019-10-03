package org.infinispan.cli.impl;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SSLContextSettings {

   private final SSLContext sslContext;
   private final KeyManager[] keyManagers;
   private final TrustManager[] trustManagers;
   private final SecureRandom random;
   private final HostnameVerifier hostnameVerifier;

   private SSLContextSettings(String protocol, KeyManager[] keyManagers, TrustManager[] trustManagers, SecureRandom random, HostnameVerifier hostnameVerifier) throws GeneralSecurityException {
      this.sslContext = SSLContext.getInstance(protocol);
      this.keyManagers = keyManagers;
      this.trustManagers = trustManagers;
      this.random = random;
      this.hostnameVerifier = hostnameVerifier;
      sslContext.init(keyManagers, trustManagers, random);
   }

   public SSLContext getSslContext() {
      return sslContext;
   }

   public KeyManager[] getKeyManagers() {
      return keyManagers;
   }

   public TrustManager[] getTrustManagers() {
      return trustManagers;
   }

   public SecureRandom getRandom() {
      return random;
   }

   public HostnameVerifier getHostnameVerifier() {
      return hostnameVerifier;
   }

   public static SSLContextSettings getInstance(String protocol, KeyManager[] keyManagers, TrustManager[] trustManagers, SecureRandom random, HostnameVerifier hostnameVerifier) {
      try {
         return new SSLContextSettings(protocol, keyManagers, trustManagers, random, hostnameVerifier);
      } catch (GeneralSecurityException e) {
         throw new RuntimeException(e);
      }
   }
}
