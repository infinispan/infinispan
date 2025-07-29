package org.infinispan.client.openapi.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.BiPredicate;

import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;

/**
 * @since 15.0
 **/
public abstract class HttpAuthenticator {
   public static final String WWW_AUTH = "www-authenticate";
   public static final String WWW_AUTH_RESP = "Authorization";
   protected final HttpClient client;
   protected final AuthenticationConfiguration configuration;

   public HttpAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      this.client = client;
      this.configuration = configuration;
   }

   public boolean supportsPreauthentication() {
      return false;
   }

   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request) {
      throw new UnsupportedOperationException();
   }

   public abstract <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler);

   public static HttpRequest.Builder copyRequest(HttpRequest request) {
      return copyRequest(request, (n, v) -> true);
   }

   public static HttpRequest.Builder copyRequest(HttpRequest request, BiPredicate<String, String> filter) {
      Objects.requireNonNull(request);
      Objects.requireNonNull(filter);

      final HttpRequest.Builder builder = HttpRequest.newBuilder();
      builder.uri(request.uri());
      builder.expectContinue(request.expectContinue());

      HttpHeaders headers = HttpHeaders.of(request.headers().map(), filter);
      headers.map().forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
      request.version().ifPresent(builder::version);
      request.timeout().ifPresent(builder::timeout);
      var method = request.method();
      request.bodyPublisher().ifPresentOrElse(
            bodyPublisher -> builder.method(method, bodyPublisher),
            () -> {
               switch (method) {
                  case "GET":
                     builder.GET();
                     break;
                  case "DELETE":
                     builder.DELETE();
                     break;
                  default:
                     builder.method(method, HttpRequest.BodyPublishers.noBody());
                     break;
               }
            }
      );
      return builder;
   }

   static String findHeader(HttpResponse<?> response, String name, String prefix) {
      final List<String> authHeaders = response.headers().allValues(name);
      for (String header : authHeaders) {
         if (header.startsWith(prefix)) {
            return header;
         }
      }
      throw new AuthenticationException("unsupported auth scheme: " + authHeaders);
   }


}
