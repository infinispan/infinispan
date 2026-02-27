package org.infinispan.server.security.http.localuser;

import static org.wildfly.security.http.HttpConstants.AUTHORIZATION;
import static org.wildfly.security.http.HttpConstants.UNAUTHORIZED;
import static org.wildfly.security.http.HttpConstants.WWW_AUTHENTICATE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;
import org.wildfly.security.http.HttpServerRequest;

/**
 * Implementation of the HTTP Local authentication mechanism.
 * Uses a challenge file on the local filesystem to prove that
 * the client is running on the same machine as the server.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
final class LocalUserAuthenticationMechanism implements HttpServerAuthenticationMechanism {
   public static final String LOCALUSER_NAME = "LOCALUSER";

   public static final String LOCAL_USER_CHALLENGE_PATH = "wildfly.http.local-user.challenge-path";
   public static final String DEFAULT_USER = "wildfly.http.local-user.default-user";

   private static final String CHALLENGE_PREFIX = "Localuser";
   private static final String SESSION_TOKEN_PREFIX = "token:";
   private static final String SESSION_TOKEN_HEADER = "X-Localuser-Token";
   private static final int CHALLENGE_BYTES = 8;
   private static final int SESSION_TOKEN_BYTES = 32;
   private static final long CHALLENGE_TIMEOUT_SECONDS = 60;
   private static final long SESSION_TOKEN_TTL_SECONDS = 3600;

   private static final ConcurrentHashMap<String, byte[]> pendingChallenges = new ConcurrentHashMap<>();
   private static final ConcurrentHashMap<String, String> sessionTokens = new ConcurrentHashMap<>();
   private static final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "localuser-challenge-cleanup");
      t.setDaemon(true);
      return t;
   });

   private final CallbackHandler callbackHandler;
   private final File challengePath;
   private final String defaultUser;

   LocalUserAuthenticationMechanism(CallbackHandler callbackHandler, String challengePath, String defaultUser) {
      this.callbackHandler = callbackHandler;
      this.challengePath = challengePath != null ? new File(challengePath) : new File(System.getProperty("java.io.tmpdir"));
      this.defaultUser = defaultUser != null ? defaultUser : "$local";
   }

   @Override
   public String getMechanismName() {
      return LOCALUSER_NAME;
   }

   @Override
   public void evaluateRequest(HttpServerRequest request) throws HttpAuthenticationException {
      List<String> authorizationValues = request.getRequestHeaderValues(AUTHORIZATION);

      if (authorizationValues == null || authorizationValues.isEmpty()) {
         // No Authorization header: advertise that we support Localuser
         request.noAuthenticationInProgress(response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
            response.setStatusCode(UNAUTHORIZED);
         });
         return;
      }

      for (String current : authorizationValues) {
         if (!current.startsWith(CHALLENGE_PREFIX)) {
            continue;
         }

         String token = current.substring(CHALLENGE_PREFIX.length()).trim();

         if (token.isEmpty()) {
            // Initial handshake: create challenge file with random bytes
            handleInitialHandshake(request);
         } else if (token.startsWith(SESSION_TOKEN_PREFIX)) {
            // Session token: validate cached token
            handleSessionToken(request, token.substring(SESSION_TOKEN_PREFIX.length()));
         } else {
            // Response round: validate the challenge response
            handleChallengeResponse(request, token);
         }
         return;
      }
   }

   private void handleInitialHandshake(HttpServerRequest request) throws HttpAuthenticationException {
      try {
         SecureRandom random = new SecureRandom();
         File challengeFile = File.createTempFile("localuser-", ".challenge", challengePath);
         byte[] bytes = new byte[CHALLENGE_BYTES];
         random.nextBytes(bytes);

         try (FileOutputStream fos = new FileOutputStream(challengeFile)) {
            fos.write(bytes);
         }

         String filePath = challengeFile.getAbsolutePath();
         pendingChallenges.put(filePath, bytes);

         // Schedule cleanup after timeout
         cleanupExecutor.schedule(() -> {
            pendingChallenges.remove(filePath);
            challengeFile.delete();
         }, CHALLENGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

         request.authenticationInProgress(response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX + " " + filePath);
            response.setStatusCode(UNAUTHORIZED);
         });
      } catch (IOException e) {
         throw new HttpAuthenticationException(e);
      }
   }

   private void handleSessionToken(HttpServerRequest request, String token) throws HttpAuthenticationException {
      String user = sessionTokens.get(token);
      if (user == null) {
         request.authenticationFailed("Unknown or expired session token", response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
            response.setStatusCode(UNAUTHORIZED);
         });
         return;
      }
      try {
         AuthorizeCallback authorizeCallback = new AuthorizeCallback(user, user);
         callbackHandler.handle(new Callback[]{authorizeCallback});
         if (authorizeCallback.isAuthorized()) {
            request.authenticationComplete();
         } else {
            request.authenticationFailed("Authorization denied for " + user, response -> {
               response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
               response.setStatusCode(UNAUTHORIZED);
            });
         }
      } catch (IOException | UnsupportedCallbackException e) {
         throw new HttpAuthenticationException(e);
      }
   }

   private void handleChallengeResponse(HttpServerRequest request, String token) throws HttpAuthenticationException {
      // Token format: "<filepath> <hex_bytes>"
      int spaceIdx = token.indexOf(' ');
      if (spaceIdx < 0) {
         request.authenticationFailed("Invalid LOCALUSER response format", response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
            response.setStatusCode(UNAUTHORIZED);
         });
         return;
      }

      String filePath = token.substring(0, spaceIdx);
      String hexBytes = token.substring(spaceIdx + 1);

      // Look up and remove the challenge
      byte[] expectedBytes = pendingChallenges.remove(filePath);

      // Delete the challenge file
      new File(filePath).delete();

      if (expectedBytes == null) {
         request.authenticationFailed("Unknown or expired challenge", response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
            response.setStatusCode(UNAUTHORIZED);
         });
         return;
      }

      byte[] receivedBytes = hexToBytes(hexBytes);

      // Constant-time comparison
      if (MessageDigest.isEqual(expectedBytes, receivedBytes)) {
         try {
            AuthorizeCallback authorizeCallback = new AuthorizeCallback(defaultUser, defaultUser);
            callbackHandler.handle(new Callback[]{authorizeCallback});
            if (authorizeCallback.isAuthorized()) {
               String sessionToken = generateSessionToken();
               sessionTokens.put(sessionToken, defaultUser);
               cleanupExecutor.schedule(() -> sessionTokens.remove(sessionToken),
                     SESSION_TOKEN_TTL_SECONDS, TimeUnit.SECONDS);
               request.authenticationComplete(response ->
                     response.addResponseHeader(SESSION_TOKEN_HEADER, sessionToken));
            } else {
               request.authenticationFailed("Authorization denied for " + defaultUser, response -> {
                  response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
                  response.setStatusCode(UNAUTHORIZED);
               });
            }
         } catch (IOException | UnsupportedCallbackException e) {
            throw new HttpAuthenticationException(e);
         }
      } else {
         request.authenticationFailed("Challenge response mismatch", response -> {
            response.addResponseHeader(WWW_AUTHENTICATE, CHALLENGE_PREFIX);
            response.setStatusCode(UNAUTHORIZED);
         });
      }
   }

   private static String generateSessionToken() {
      SecureRandom random = new SecureRandom();
      byte[] bytes = new byte[SESSION_TOKEN_BYTES];
      random.nextBytes(bytes);
      return bytesToHex(bytes);
   }

   private static String bytesToHex(byte[] bytes) {
      char[] hexChars = new char[bytes.length * 2];
      for (int i = 0; i < bytes.length; i++) {
         int v = bytes[i] & 0xFF;
         hexChars[i * 2] = Character.forDigit(v >>> 4, 16);
         hexChars[i * 2 + 1] = Character.forDigit(v & 0x0F, 16);
      }
      return new String(hexChars);
   }

   private static byte[] hexToBytes(String hex) {
      int len = hex.length();
      byte[] data = new byte[len / 2];
      for (int i = 0; i < len; i += 2) {
         data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
               + Character.digit(hex.charAt(i + 1), 16));
      }
      return data;
   }

   @Override
   public void dispose() {
      // Nothing to dispose
   }
}
