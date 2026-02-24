package org.infinispan.client.rest.impl.jdk.auth;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.commons.util.Util;

/**
 * Client-side authenticator for the LOCALUSER HTTP authentication mechanism.
 * Performs a multi-round challenge-response flow:
 * <ol>
 *   <li>On 401 with {@code WWW-Authenticate: Localuser} (no filepath): send {@code Authorization: Localuser} to trigger challenge creation</li>
 *   <li>On 401 with {@code WWW-Authenticate: Localuser <filepath>}: read the file, hex-encode the bytes, send {@code Authorization: Localuser <filepath> <hex_bytes>}</li>
 * </ol>
 *
 * @since 16.2
 */
public class LocalUserAuthenticator extends HttpAuthenticator {
   private static final String LOCALUSER_PREFIX = "Localuser";
   private static final String SESSION_TOKEN_PREFIX = "token:";
   private static final String SESSION_TOKEN_HEADER = "X-Localuser-Token";
   private static final long MAX_CHALLENGE_FILE_SIZE = 1024;

   // Guard against infinite retry loops
   private int retryCount = 0;
   private static final int MAX_RETRIES = 3;

   private volatile String cachedToken;

   public LocalUserAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
   }

   @Override
   public boolean supportsPreauthentication() {
      return cachedToken != null;
   }

   @Override
   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request) {
      String token = cachedToken;
      if (token != null) {
         request.header(WWW_AUTH_RESP, LOCALUSER_PREFIX + " " + SESSION_TOKEN_PREFIX + token);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      // If the rejected request used a session token, invalidate it and restart
      HttpRequest originalRequest = response.request();
      String authHeader = originalRequest.headers().firstValue(WWW_AUTH_RESP).orElse(null);
      if (authHeader != null && authHeader.contains(SESSION_TOKEN_PREFIX)) {
         cachedToken = null;
         retryCount = 0;
      }

      if (retryCount >= MAX_RETRIES) {
         return null;
      }
      retryCount++;

      List<String> wwwAuthHeaders = response.headers().allValues(WWW_AUTH);
      String localuserHeader = null;
      for (String header : wwwAuthHeaders) {
         if (header.startsWith(LOCALUSER_PREFIX)) {
            localuserHeader = header;
            break;
         }
      }

      if (localuserHeader == null) {
         return null;
      }

      String token = localuserHeader.substring(LOCALUSER_PREFIX.length()).trim();

      HttpRequest.Builder newRequest = copyRequest(originalRequest, (n, v) -> !n.equalsIgnoreCase(WWW_AUTH_RESP));

      if (token.isEmpty()) {
         // Round 1: send empty Localuser authorization to trigger challenge creation
         newRequest.header(WWW_AUTH_RESP, LOCALUSER_PREFIX);
      } else {
         // Round 2: read challenge file and send back the bytes
         try {
            Path challengeFile = Path.of(token).toRealPath();
            Path tempDir = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
            if (!challengeFile.startsWith(tempDir)) {
               throw new AuthenticationException("LOCALUSER challenge file is not in the temp directory: " + challengeFile);
            }
            long size = Files.size(challengeFile);
            if (size > MAX_CHALLENGE_FILE_SIZE) {
               throw new AuthenticationException("LOCALUSER challenge file exceeds maximum size: " + size);
            }
            byte[] challengeBytes = Files.readAllBytes(challengeFile);
            String hexEncoded = Util.toHexString(challengeBytes);
            newRequest.header(WWW_AUTH_RESP, LOCALUSER_PREFIX + " " + token + " " + hexEncoded);
         } catch (IOException e) {
            throw new AuthenticationException("Failed to read LOCALUSER challenge file: " + token, e);
         }
      }

      return client.sendAsync(newRequest.build(), (HttpResponse.BodyHandler<T>) bodyHandler)
            .thenApply(r -> {
               if (r.statusCode() != 401) {
                  retryCount = 0;
                  r.headers().firstValue(SESSION_TOKEN_HEADER).ifPresent(t -> cachedToken = t);
               }
               return r;
            });
   }

}
