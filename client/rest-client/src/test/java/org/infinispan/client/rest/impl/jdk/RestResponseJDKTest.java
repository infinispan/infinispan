package org.infinispan.client.rest.impl.jdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSession;

import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RestResponseJDK}, which wraps a JDK {@link HttpResponse}.
 */
class RestResponseJDKTest {

   @Test
   void statusAndHeader() {
      HttpResponse<String> mock = mockResponse(200, Map.of("Content-Type", List.of("application/json")), "body", HttpClient.Version.HTTP_1_1, mockRequest());
      try (RestResponseJDK<String> response = new RestResponseJDK<>(mock)) {
         assertThat(response.status()).isEqualTo(200);
         assertThat(response.header("Content-Type")).isEqualTo("application/json");
         assertThat(response.header("Missing")).isNull();
      }
   }

   @Test
   void bodyAsString() {
      HttpResponse<String> mock = mockResponse(200, Map.of(), "hello", HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new RestResponseJDK<>(mock).body()).isEqualTo("hello");
   }

   @Test
   void bodyAsByteArray() {
      HttpResponse<byte[]> mock = mockResponse(200, Map.of(), "hello".getBytes(StandardCharsets.UTF_8), HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new RestResponseJDK<>(mock).bodyAsByteArray()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
   }

   @Test
   void bodyAsStream() throws Exception {
      HttpResponse<String> mock = mockResponse(200, Map.of(), "hello", HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new String(new RestResponseJDK<>(mock).bodyAsStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("hello");
   }

   @Test
   void protocol() {
      HttpResponse<String> http1 = mockResponse(200, Map.of(), "body", HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new RestResponseJDK<>(http1).protocol()).isEqualTo(Protocol.HTTP_11);
      HttpResponse<String> http2 = mockResponse(200, Map.of(), "body", HttpClient.Version.HTTP_2, mockRequest());
      assertThat(new RestResponseJDK<>(http2).protocol()).isEqualTo(Protocol.HTTP_20);
   }

   @Test
   void contentType() {
      HttpResponse<String> mock = mockResponse(200, Map.of("Content-Type", List.of("application/json")), "body", HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new RestResponseJDK<>(mock).contentType()).isEqualTo(MediaType.APPLICATION_JSON);
   }

   @Test
   void usedAuthentication() {
      HttpRequest withAuth = HttpRequest.newBuilder().uri(URI.create("http://localhost/")).header("Authorization", "Basic xyz").build();
      HttpResponse<String> mock = mockResponse(200, Map.of(), "body", HttpClient.Version.HTTP_1_1, withAuth);
      assertThat(new RestResponseJDK<>(mock).usedAuthentication()).isTrue();

      HttpResponse<String> mockNoAuth = mockResponse(200, Map.of(), "body", HttpClient.Version.HTTP_1_1, mockRequest());
      assertThat(new RestResponseJDK<>(mockNoAuth).usedAuthentication()).isFalse();
   }

   private static HttpRequest mockRequest() {
      return HttpRequest.newBuilder().uri(URI.create("http://localhost/")).GET().build();
   }

   private static <T> HttpResponse<T> mockResponse(int status, Map<String, List<String>> headers, T body, HttpClient.Version version, HttpRequest request) {
      HttpHeaders httpHeaders = HttpHeaders.of(headers, (n, v) -> true);
      return new HttpResponse<>() {
         @Override
         public HttpHeaders headers() {
            return httpHeaders;
         }

         @Override
         public int statusCode() {
            return status;
         }

         @Override
         public HttpRequest request() {
            return request;
         }

         @Override
         public T body() {
            return body;
         }

         @Override
         public HttpClient.Version version() {
            return version;
         }

         @Override
         public Optional<HttpResponse<T>> previousResponse() {
            return Optional.empty();
         }

         @Override
         public Optional<SSLSession> sslSession() {
            return Optional.empty();
         }

         @Override
         public URI uri() {
            return request.uri();
         }
      };
   }
}
