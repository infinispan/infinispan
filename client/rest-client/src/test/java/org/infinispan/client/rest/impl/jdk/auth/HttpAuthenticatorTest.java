package org.infinispan.client.rest.impl.jdk.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the stateless {@link HttpAuthenticator} helpers.
 */
class HttpAuthenticatorTest {

   @Test
   void copyRequestPreservesUriMethodAndHeaders() {
      HttpRequest original = HttpRequest.newBuilder().uri(URI.create("http://localhost/path")).header("X-Test", "value").GET().build();
      HttpRequest copied = HttpAuthenticator.copyRequest(original).build();
      assertThat(copied.uri()).isEqualTo(URI.create("http://localhost/path"));
      assertThat(copied.method()).isEqualTo("GET");
      assertThat(copied.headers().firstValue("X-Test")).contains("value");
   }

   @Test
   void copyRequestFiltersHeaders() {
      HttpRequest original = HttpRequest.newBuilder().uri(URI.create("http://localhost/path"))
            .header("Authorization", "Basic xyz").header("X-Keep", "value").GET().build();
      HttpRequest copied = HttpAuthenticator.copyRequest(original, (n, v) -> !n.equalsIgnoreCase("Authorization")).build();
      assertThat(copied.headers().firstValue("Authorization")).isEmpty();
      assertThat(copied.headers().firstValue("X-Keep")).contains("value");
   }

   @Test
   void copyRequestPreservesBody() {
      HttpRequest original = HttpRequest.newBuilder().uri(URI.create("http://localhost/path"))
            .POST(HttpRequest.BodyPublishers.ofString("body", StandardCharsets.UTF_8)).build();
      HttpRequest copied = HttpAuthenticator.copyRequest(original).build();
      assertThat(copied.method()).isEqualTo("POST");
      assertThat(new String(publishBody(copied.bodyPublisher().get()), StandardCharsets.UTF_8)).isEqualTo("body");
   }

   private static byte[] publishBody(HttpRequest.BodyPublisher publisher) {
      StringBuilder sb = new StringBuilder();
      publisher.subscribe(new Flow.Subscriber<>() {
         private Flow.Subscription subscription;

         @Override
         public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            s.request(1);
         }

         @Override
         public void onNext(ByteBuffer b) {
            byte[] arr = new byte[b.remaining()];
            b.get(arr);
            sb.append(new String(arr, StandardCharsets.UTF_8));
            subscription.request(1);
         }

         @Override
         public void onError(Throwable t) {
            throw new RuntimeException(t);
         }

         @Override
         public void onComplete() {
         }
      });
      return sb.toString().getBytes(StandardCharsets.UTF_8);
   }
}
