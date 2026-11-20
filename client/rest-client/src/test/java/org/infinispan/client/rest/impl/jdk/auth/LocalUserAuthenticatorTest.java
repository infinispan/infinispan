package org.infinispan.client.rest.impl.jdk.auth;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Verifies the challenge-response flow of {@link LocalUserAuthenticator} under concurrency: the per-request
 * retry budget must not be shared between requests, and the session token cache must be invalidated and
 * re-established independently per request.
 */
class LocalUserAuthenticatorTest {

   private static final String TOKEN_HEADER = "X-Localuser-Token";
   private static final String LOCALUSER = "Localuser";
   private static final String TOKEN_PREFIX = "token:";

   @Test
   void concurrentRequestsCompleteChallengeResponseIndependently() throws Exception {
      int numRequests = 20;
      ChallengeServer server = startChallengeServer();
      try (RestClient client = createClient(server)) {
         List<CompletableFuture<RestResponse>> futures = new ArrayList<>(numRequests);
         for (int i = 0; i < numRequests; i++) {
            futures.add(client.caches().toCompletableFuture());
         }
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, SECONDS);
         for (CompletableFuture<RestResponse> future : futures) {
            assertThat(future.get().status()).isEqualTo(RestResponse.OK);
         }
         // Every request must have completed the two-round file exchange independently; a retry budget shared
         // between requests would make some of them give up before completing it.
         assertThat(server.successes).hasValue(numRequests);
      } finally {
         server.stop();
      }
   }

   @Test
   void sessionTokenIsCachedAndReused() throws Exception {
      ChallengeServer server = startChallengeServer();
      try (RestClient client = createClient(server)) {
         var first = client.caches().toCompletableFuture().get(30, SECONDS);
         assertThat(first.status()).isEqualTo(RestResponse.OK);
         var second = client.caches().toCompletableFuture().get(30, SECONDS);
         assertThat(second.status()).isEqualTo(RestResponse.OK);
         // Only the first request performs the file exchange; the second is preauthenticated with the cached token.
         assertThat(server.successes).hasValue(1);
         assertThat(server.tokenPreauths).hasValue(1);
      } finally {
         server.stop();
      }
   }

   @Test
   void rejectedTokenRestartsChallengeIndependently() throws Exception {
      int numRequests = 10;
      ChallengeServer server = startChallengeServer();
      try (RestClient client = createClient(server)) {
         var first = client.caches().toCompletableFuture().get(30, SECONDS);
         assertThat(first.status()).isEqualTo(RestResponse.OK);
         // From now on the server rejects session tokens; every request must invalidate the cached token and
         // restart the challenge-response exchange independently.
         server.rejectTokens = true;
         List<CompletableFuture<RestResponse>> futures = new ArrayList<>(numRequests);
         for (int i = 0; i < numRequests; i++) {
            futures.add(client.caches().toCompletableFuture());
         }
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, SECONDS);
         for (CompletableFuture<RestResponse> future : futures) {
            assertThat(future.get().status()).isEqualTo(RestResponse.OK);
         }
         assertThat(server.successes).hasValue(numRequests + 1);
      } finally {
         server.stop();
      }
   }

   private static ChallengeServer startChallengeServer() throws IOException {
      HttpServer httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      ChallengeServer server = new ChallengeServer(httpServer);
      httpServer.createContext("/", server::handle);
      httpServer.start();
      return server;
   }

   private static RestClient createClient(ChallengeServer server) {
      String uri = "http://localhost:" + server.httpServer.getAddress().getPort();
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.uri(uri).pingOnCreate(false);
      restBuilder.security().authentication()
            .mechanism("LOCALUSER");
      return new org.infinispan.client.rest.impl.jdk.RestClientJDK(restBuilder.build());
   }

   /**
    * Simulates the LOCALUSER server side: it issues a plain challenge, then a challenge naming a file with
    * random content in the temp directory, and accepts only a request echoing the exact hex-encoded content
    * of that file. Successful requests are answered with a session token which subsequent requests may use
    * unless {@link #rejectTokens} is set.
    */
   private static final class ChallengeServer {
      final HttpServer httpServer;
      volatile boolean rejectTokens;
      final AtomicInteger successes = new AtomicInteger();
      final AtomicInteger tokenPreauths = new AtomicInteger();
      final Map<String, byte[]> challengeFiles = new ConcurrentHashMap<>();
      final Set<String> issuedTokens = ConcurrentHashMap.newKeySet();

      ChallengeServer(HttpServer httpServer) {
         this.httpServer = httpServer;
      }

      void handle(HttpExchange exchange) throws IOException {
         String authorization = exchange.getRequestHeaders().getFirst("Authorization");
         try (exchange) {
            if (authorization != null && authorization.startsWith(LOCALUSER + " " + TOKEN_PREFIX)) {
               String token = authorization.substring((LOCALUSER + " " + TOKEN_PREFIX).length()).trim();
               if (!rejectTokens && issuedTokens.contains(token)) {
                  tokenPreauths.incrementAndGet();
                  sendOk(exchange);
                  return;
               }
               sendChallenge(exchange, null);
            } else if (LOCALUSER.equals(authorization)) {
               sendFileChallenge(exchange);
            } else if (authorization != null && authorization.startsWith(LOCALUSER + " ")) {
               if (verifyRoundTwo(authorization)) {
                  String token = "token-" + UUID.randomUUID();
                  issuedTokens.add(token);
                  exchange.getResponseHeaders().set(TOKEN_HEADER, token);
                  successes.incrementAndGet();
                  sendOk(exchange);
               } else {
                  sendFileChallenge(exchange);
               }
            } else {
               sendChallenge(exchange, null);
            }
         }
      }

      private boolean verifyRoundTwo(String authorization) {
         String[] parts = authorization.split(" ", 3);
         if (parts.length < 3) {
            return false;
         }
         byte[] expected = challengeFiles.get(parts[1]);
         return expected != null && Util.toHexString(expected).equalsIgnoreCase(parts[2]);
      }

      private void sendChallenge(HttpExchange exchange, String challengeFile) throws IOException {
         exchange.getResponseHeaders().add("WWW-Authenticate", challengeFile == null ? LOCALUSER : LOCALUSER + " " + challengeFile);
         exchange.sendResponseHeaders(401, -1);
      }

      private void sendFileChallenge(HttpExchange exchange) throws IOException {
         byte[] content = new byte[16];
         new SecureRandom().nextBytes(content);
         String file = Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString()).toString();
         try {
            Files.write(Path.of(file), content);
         } catch (IOException e) {
            throw new IOException("Unable to write challenge file", e);
         }
         challengeFiles.put(file, content);
         sendChallenge(exchange, file);
      }

      private static void sendOk(HttpExchange exchange) throws IOException {
         byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
         exchange.getResponseHeaders().set("Content-Type", "text/plain");
         exchange.sendResponseHeaders(200, body.length);
         try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
         }
      }

      void stop() {
         for (String file : challengeFiles.keySet()) {
            try {
               Files.deleteIfExists(Path.of(file));
            } catch (IOException ignored) {
            }
         }
         httpServer.stop(0);
      }
   }
}
