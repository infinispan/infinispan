package org.infinispan.client.rest.impl.jdk.auth;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Verifies that {@link AutoDetectAuthenticator} falls back to BASIC when the server advertises both DIGEST and BASIC but digest verification fails, e.g. against an LDAP realm whose passwords are stored in a scheme which cannot be verified for digest purposes (see #18435). The simulated server mirrors Undertow's behavior of pruning its re-challenges after a mechanism has failed: once the digest attempt is rejected it advertises DIGEST only, so the client must remember that BASIC was challenged earlier in the exchange.
 */
class AutoDetectAuthenticatorTest {

   private static final String USERNAME = "user";
   private static final String PASSWORD = "pass";
   // Cap the number of requests so that a client with broken fallback logic fails fast instead of looping forever
   private static final int MAX_REQUESTS = 5;

   @Test
   void shouldFallBackToBasicWhenDigestFails() throws Exception {
      List<List<String>> observedAuthorizationHeaders = new CopyOnWriteArrayList<>();
      HttpServer server = startChallengeServer(observedAuthorizationHeaders);
      try (RestClient client = createClient(server)) {
         var response = client.caches().toCompletableFuture().get(30, SECONDS);
         assertThat(response.status()).isEqualTo(org.infinispan.client.rest.RestResponse.OK);
         assertThat(response.body()).contains("authenticated");

         List<List<String>> requests = new ArrayList<>(observedAuthorizationHeaders);
         // The unauthenticated request is challenged, the digest attempt is rejected and only then a single clean BASIC retry succeeds
         assertThat(requests).hasSize(3);
         assertThat(requests.get(0)).isEmpty();
         assertThat(requests.get(1)).hasSize(1);
         assertThat(requests.get(2)).hasSize(1);
         assertThat(requests.get(1).get(0)).startsWith("Digest ");
         assertThat(requests.get(2).get(0)).startsWith("Basic ");
      } finally {
         server.stop(0);
      }
   }

   @Test
   void shouldHandleConcurrentRequestsIndependently() throws Exception {
      int numRequests = 20;
      HttpServer server = startBasicOnlyServer();
      try (RestClient client = createClient(server)) {
         List<CompletableFuture<RestResponse>> futures = new ArrayList<>(numRequests);
         for (int i = 0; i < numRequests; i++) {
            futures.add(client.caches().toCompletableFuture());
         }
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, SECONDS);
         for (CompletableFuture<RestResponse> future : futures) {
            assertThat(future.get().status()).isEqualTo(RestResponse.OK);
         }
      } finally {
         server.stop(0);
      }
   }

   /**
    * A stateless server which rejects every request that does not carry exactly one Basic credential (in particular digest attempts) and re-challenges with both DIGEST and BASIC, but accepts a clean Basic credential. Because the handler keeps no per-request state, it is safe under concurrency and each client request must independently complete the digest-then-basic fallback.
    */
   private static HttpServer startBasicOnlyServer() throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/", AutoDetectAuthenticatorTest::handleBasicOnly);
      server.start();
      return server;
   }

   private static void handleBasicOnly(HttpExchange exchange) throws IOException {
      List<String> authorization = exchange.getRequestHeaders().get("Authorization");
      boolean singleBasic = authorization != null && authorization.size() == 1 && authorization.get(0).startsWith("Basic ");
      try (exchange) {
         if (singleBasic) {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
               os.write(body);
            }
         } else {
            exchange.getResponseHeaders().add("WWW-Authenticate", "Digest realm=\"TestRealm\", nonce=abc123");
            exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"TestRealm\"");
            exchange.sendResponseHeaders(401, -1);
         }
      }
   }

   private static RestClient createClient(HttpServer server) {
      String uri = "http://localhost:" + server.getAddress().getPort();
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.uri(uri).pingOnCreate(false);
      restBuilder.security().authentication()
            .mechanism("AUTO")
            .username(USERNAME)
            .password(PASSWORD);
      return new org.infinispan.client.rest.impl.jdk.RestClientJDK(restBuilder.build());
   }

   /**
    * Simulates a server which advertises both DIGEST and BASIC challenges, rejects any digest attempt (as an LDAP backend storing PBKDF2 hashes would) re-challenging with DIGEST only afterwards (as Undertow does), and accepts only requests carrying exactly one Authorization header with the Basic scheme. Any other combination of credentials is challenged again until {@link #MAX_REQUESTS} has been reached, after which challenges stop so that a client with broken fallback logic terminates instead of looping forever.
    */
   private static HttpServer startChallengeServer(List<List<String>> observedAuthorizationHeaders) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/", exchange -> handle(exchange, observedAuthorizationHeaders));
      server.start();
      return server;
   }

   private static void handle(HttpExchange exchange, List<List<String>> observedAuthorizationHeaders) throws IOException {
      List<String> headerValues = exchange.getRequestHeaders().get("Authorization");
      // A request carrying multiple values for the Authorization header is invalid per RFC 9110 and can never authenticate; record every value as observed on the wire
      List<String> authorization = (headerValues == null || headerValues.isEmpty()) ? List.of() : List.copyOf(headerValues);

      boolean singleBasic = authorization.size() == 1 && authorization.get(0).startsWith("Basic ");
      boolean singleDigest = authorization.size() == 1 && authorization.get(0).startsWith("Digest ");
      try (exchange) {
         if (singleBasic) {
            // Only a request carrying exactly one clean Basic credential can authenticate; an accumulated Digest value alongside it is invalid per RFC 9110 and must be rejected
            byte[] body = "authenticated".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
               os.write(body);
            }
         } else if (observedAuthorizationHeaders.size() + 1 > MAX_REQUESTS) {
            // Stop challenging so that a client with broken fallback logic terminates instead of looping forever
            exchange.sendResponseHeaders(401, -1);
         } else {
            // The digest attempt is rejected as an LDAP backend storing PBKDF2 hashes would do; like Undertow the server prunes its re-challenges to the failed scheme only (see #18435)
            challenge(exchange, !singleDigest);
         }
      } finally {
         observedAuthorizationHeaders.add(authorization);
      }
   }

   private static void challenge(HttpExchange exchange, boolean advertiseBasic) throws IOException {
      // Advertised in the same order as configured on a server: DIGEST first (see #18435), BASIC second unless pruned after its re-challenge follows a failed digest attempt
      exchange.getResponseHeaders().add("WWW-Authenticate", "Digest realm=\"TestRealm\", nonce=0a4f113b");
      if (advertiseBasic) {
         exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"TestRealm\"");
      }
      exchange.sendResponseHeaders(401, -1);
   }
}
