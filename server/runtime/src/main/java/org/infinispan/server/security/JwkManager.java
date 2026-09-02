package org.infinispan.server.security;

import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

/**
 * Caches RSA JSON Web Keys fetched from a JWKS endpoint for JWT signature validation.
 * <p>
 * Adapted from Elytron's JwkManager. This is a temporary copy until the Elytron
 * <a href="https://issues.redhat.com/browse/ELY-2911">ELY-2911</a> enhancement is released.
 *
 * @since 16.2
 */
public class JwkManager {

   private static final Log log = LogFactory.getLog(JwkManager.class);

   private final Map<URL, CacheEntry> keys = new LinkedHashMap<>();
   private final SSLContext sslContext;
   private final HostnameVerifier hostnameVerifier;

   private final long updateTimeout;
   private final int minTimeBetweenRequests;

   private final int connectionTimeout;
   private final int readTimeout;

   public JwkManager(SSLContext sslContext, HostnameVerifier hostnameVerifier, long updateTimeout, int connectionTimeout, int readTimeout) {
      this(sslContext, hostnameVerifier, updateTimeout, connectionTimeout, readTimeout, 2000);
   }

   public JwkManager(SSLContext sslContext, HostnameVerifier hostnameVerifier, long updateTimeout, int connectionTimeout, int readTimeout, int minTimeBetweenRequests) {
      this.sslContext = sslContext;
      this.hostnameVerifier = hostnameVerifier;
      this.updateTimeout = updateTimeout;
      this.connectionTimeout = connectionTimeout;
      this.readTimeout = readTimeout;
      this.minTimeBetweenRequests = minTimeBetweenRequests;
   }

   /**
    * Thread-safe method for receiving a remote public key.
    *
    * @param kid key id
    * @param url remote JWKS URL
    * @return signature verification public key if found, null otherwise
    */
   public PublicKey getPublicKey(String kid, URL url) {
      Map<String, RSAPublicKey> urlKeys = checkRemote(kid, url);
      if (urlKeys == null) {
         return null;
      }
      PublicKey pk = urlKeys.get(kid);
      if (pk == null) {
         log.warnf("Unknown kid: %s", kid);
      }
      return pk;
   }

   /**
    * Thread-safe method for retrieving all cached keys for a URL, refreshing if needed.
    *
    * @param url remote JWKS URL
    * @return map of kid to public key, or empty map if fetch fails
    */
   public Map<String, RSAPublicKey> getKeys(URL url) {
      CacheEntry cacheEntry;
      long lastUpdate;
      Map<String, RSAPublicKey> urlKeys;

      synchronized (keys) {
         cacheEntry = keys.get(url);
         if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            keys.put(url, cacheEntry);
         }
         lastUpdate = cacheEntry.getTimestamp();
         urlKeys = cacheEntry.getKeys();
      }

      long currentTime = System.currentTimeMillis();

      if (!urlKeys.isEmpty() && lastUpdate + updateTimeout > currentTime) {
         return urlKeys;
      }

      if (lastUpdate + minTimeBetweenRequests > currentTime) {
         return urlKeys;
      }

      synchronized (cacheEntry) {
         if ((cacheEntry.getKeys().isEmpty() || cacheEntry.getTimestamp() + updateTimeout <= currentTime)
               && cacheEntry.getTimestamp() + minTimeBetweenRequests <= currentTime) {
            Map<String, RSAPublicKey> newJwks = fetchJwksFromUrl(url);
            if (newJwks == null) {
               log.warnf("Unable to fetch JWKS from %s", url);
               return urlKeys;
            }
            cacheEntry.setKeys(newJwks);
            cacheEntry.setTimestamp(currentTime);
         }
         return cacheEntry.getKeys();
      }
   }

   private Map<String, RSAPublicKey> checkRemote(String kid, URL url) {
      CacheEntry cacheEntry;
      long lastUpdate;
      Map<String, RSAPublicKey> urlKeys;

      synchronized (keys) {
         cacheEntry = keys.get(url);
         if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            keys.put(url, cacheEntry);
         }
         lastUpdate = cacheEntry.getTimestamp();
         urlKeys = cacheEntry.getKeys();
      }

      long currentTime = System.currentTimeMillis();

      if (urlKeys.containsKey(kid) && lastUpdate + updateTimeout > currentTime) {
         return urlKeys;
      }

      if (lastUpdate + minTimeBetweenRequests > currentTime) {
         return urlKeys;
      }

      synchronized (cacheEntry) {
         if ((!cacheEntry.getKeys().containsKey(kid) || cacheEntry.getTimestamp() + updateTimeout <= currentTime)
               && cacheEntry.getTimestamp() + minTimeBetweenRequests <= currentTime) {
            Map<String, RSAPublicKey> newJwks = fetchJwksFromUrl(url);
            if (newJwks == null) {
               log.warnf("Unable to fetch JWKS from %s", url);
               return null;
            }
            cacheEntry.setKeys(newJwks);
            cacheEntry.setTimestamp(currentTime);
         }
         return cacheEntry.getKeys();
      }
   }

   private Map<String, RSAPublicKey> fetchJwksFromUrl(URL url) {
      try {
         URLConnection connection = url.openConnection();
         if (connection instanceof HttpsURLConnection https) {
            https.setRequestMethod("GET");
            if (sslContext != null) {
               https.setSSLSocketFactory(sslContext.getSocketFactory());
            }
            if (hostnameVerifier != null) {
               https.setHostnameVerifier(hostnameVerifier);
            }
         }
         connection.setConnectTimeout(connectionTimeout);
         connection.setReadTimeout(readTimeout);
         connection.connect();
         try (InputStream inputStream = connection.getInputStream();
              JsonReader jsonReader = Json.createReader(inputStream)) {
            JsonObject response = jsonReader.readObject();
            return parseJwks(response);
         }
      } catch (Exception e) {
         log.warnf("Unable to connect to %s: %s", url, e.getMessage());
         return null;
      }
   }

   private static Map<String, RSAPublicKey> parseJwks(JsonObject response) {
      JsonArray jwks = response.getJsonArray("keys");
      if (jwks == null) {
         log.warn("Unable to parse JWKS: missing 'keys' array");
         return null;
      }
      Map<String, RSAPublicKey> result = new LinkedHashMap<>();
      Base64.Decoder urlDecoder = Base64.getUrlDecoder();
      for (int i = 0; i < jwks.size(); i++) {
         JsonObject jwk = jwks.getJsonObject(i);
         String kid = jwk.getString("kid", null);
         String kty = jwk.getString("kty", null);
         String e = jwk.getString("e", null);
         String n = jwk.getString("n", null);

         if (kid == null || !"RSA".equals(kty) || e == null || n == null) {
            continue;
         }

         try {
            BigInteger modulus = new BigInteger(1, urlDecoder.decode(n));
            BigInteger exponent = new BigInteger(1, urlDecoder.decode(e));
            RSAPublicKey publicKey = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(new RSAPublicKeySpec(modulus, exponent));
            result.put(kid, publicKey);
         } catch (Exception ex) {
            log.warnf("Failed to parse JWK with kid '%s': %s", kid, ex.getMessage());
         }
      }
      return result;
   }

   private static class CacheEntry {
      private Map<String, RSAPublicKey> keys;
      private long timestamp;

      CacheEntry() {
         this.keys = Collections.emptyMap();
         this.timestamp = 0;
      }

      Map<String, RSAPublicKey> getKeys() {
         return keys;
      }

      long getTimestamp() {
         return timestamp;
      }

      void setKeys(Map<String, RSAPublicKey> keys) {
         this.keys = keys;
      }

      void setTimestamp(long timestamp) {
         this.timestamp = timestamp;
      }
   }
}
