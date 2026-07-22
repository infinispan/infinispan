package org.infinispan.server.security;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.wildfly.security.auth.realm.token.TokenValidator;
import org.wildfly.security.auth.realm.token.validator.JwtValidator;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.evidence.BearerTokenEvidence;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;

/**
 * A {@link TokenValidator} that wraps a {@link JwtValidator} and periodically refreshes
 * JWKS keys from the issuer's OIDC discovery endpoint using a {@link JwkManager}.
 * <p>
 * This is needed because {@link JwtValidator}'s {@code namedKeys} are immutable after construction,
 * and KeyCloak JWTs include a {@code kid} header but not a {@code jku} header.
 *
 * @since 16.2
 */
public class JwksTokenValidator implements TokenValidator {

   private static final Log log = LogFactory.getLog(JwksTokenValidator.class);

   private final JwtValidator.Builder validatorTemplate;
   private final JwkManager jwkManager;
   private final URL[] jwksUrls;

   private volatile JwtValidator delegate;
   private volatile Map<String, RSAPublicKey> lastKeys;

   private JwksTokenValidator(JwtValidator.Builder validatorTemplate, JwkManager jwkManager, URL[] jwksUrls) {
      this.validatorTemplate = validatorTemplate;
      this.jwkManager = jwkManager;
      this.jwksUrls = jwksUrls;
      this.lastKeys = Map.of();
      refreshDelegate();
   }

   @Override
   public Attributes validate(BearerTokenEvidence evidence) throws RealmUnavailableException {
      refreshDelegate();
      return delegate.validate(evidence);
   }

   private synchronized void refreshDelegate() {
      Map<String, PublicKey> allKeys = new LinkedHashMap<>();
      for (URL jwksUrl : jwksUrls) {
         Map<String, RSAPublicKey> keys = jwkManager.getKeys(jwksUrl);
         allKeys.putAll(keys);
      }
      if (!allKeys.equals(lastKeys)) {
         validatorTemplate.publicKeys(allKeys);
         delegate = (JwtValidator) validatorTemplate.build();
         lastKeys = new LinkedHashMap<>();
         for (Map.Entry<String, PublicKey> entry : allKeys.entrySet()) {
            lastKeys.put(entry.getKey(), (RSAPublicKey) entry.getValue());
         }
      }
   }

   public static JwksTokenValidator create(JwtValidator.Builder validatorBuilder, JwkManager jwkManager,
                                            String[] issuers, SSLContext sslContext, HostnameVerifier hostnameVerifier,
                                            int connectionTimeout, int readTimeout) {
      Map<String, URL> jwksUrlMap = new LinkedHashMap<>();
      for (String issuer : issuers) {
         try {
            URL jwksUrl = discoverJwksUri(issuer, sslContext, hostnameVerifier, connectionTimeout, readTimeout);
            if (jwksUrl != null) {
               jwksUrlMap.put(issuer, jwksUrl);
            }
         } catch (Exception e) {
            log.warnf("Failed to discover JWKS URI for issuer '%s': %s", issuer, e.getMessage());
         }
      }
      if (jwksUrlMap.isEmpty()) {
         return null;
      }
      return new JwksTokenValidator(validatorBuilder, jwkManager, jwksUrlMap.values().toArray(URL[]::new));
   }

   private static URL discoverJwksUri(String issuer, SSLContext sslContext, HostnameVerifier hostnameVerifier,
                                       int connTimeout, int rdTimeout) throws Exception {
      String discoveryUrl = issuer + "/.well-known/openid-configuration";
      HttpURLConnection connection = (HttpURLConnection) new URL(discoveryUrl).openConnection();
      if (connection instanceof HttpsURLConnection https) {
         if (sslContext != null) {
            https.setSSLSocketFactory(sslContext.getSocketFactory());
         }
         if (hostnameVerifier != null) {
            https.setHostnameVerifier(hostnameVerifier);
         }
      }
      connection.setConnectTimeout(connTimeout);
      connection.setReadTimeout(rdTimeout);
      try (InputStream is = connection.getInputStream(); JsonReader reader = Json.createReader(is)) {
         JsonObject discovery = reader.readObject();
         String jwksUri = discovery.getString("jwks_uri", null);
         return jwksUri != null ? new URL(jwksUri) : null;
      } finally {
         connection.disconnect();
      }
   }
}
