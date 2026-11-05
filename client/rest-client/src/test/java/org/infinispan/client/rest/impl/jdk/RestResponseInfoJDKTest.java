package org.infinispan.client.rest.impl.jdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RestResponseInfoJDK}, which wraps a JDK {@link HttpResponse.ResponseInfo}.
 */
class RestResponseInfoJDKTest {

   @Test
   void statusAndHeader() {
      HttpResponse.ResponseInfo info = mockResponseInfo(404, Map.of("Content-Type", List.of("text/plain")), HttpClient.Version.HTTP_1_1);
      RestResponseInfoJDK response = new RestResponseInfoJDK(info);
      assertThat(response.status()).isEqualTo(404);
      assertThat(response.header("Content-Type")).isEqualTo("text/plain");
      assertThat(response.header("Missing")).isNull();
   }

   @Test
   void protocol() {
      assertThat(new RestResponseInfoJDK(mockResponseInfo(200, Map.of(), HttpClient.Version.HTTP_1_1)).protocol()).isEqualTo(Protocol.HTTP_11);
      assertThat(new RestResponseInfoJDK(mockResponseInfo(200, Map.of(), HttpClient.Version.HTTP_2)).protocol()).isEqualTo(Protocol.HTTP_20);
   }

   @Test
   void contentType() {
      HttpResponse.ResponseInfo info = mockResponseInfo(200, Map.of("Content-Type", List.of("application/json")), HttpClient.Version.HTTP_1_1);
      assertThat(new RestResponseInfoJDK(info).contentType()).isEqualTo(MediaType.APPLICATION_JSON);
   }

   @Test
   void nullVersionThrows() {
      HttpResponse.ResponseInfo info = mockResponseInfo(200, Map.of(), null);
      assertThatThrownBy(() -> new RestResponseInfoJDK(info).protocol()).isInstanceOf(NullPointerException.class);
   }

   private static HttpResponse.ResponseInfo mockResponseInfo(int status, Map<String, List<String>> headers, HttpClient.Version version) {
      HttpHeaders httpHeaders = HttpHeaders.of(headers, (n, v) -> true);
      return new HttpResponse.ResponseInfo() {
         @Override
         public HttpHeaders headers() {
            return httpHeaders;
         }

         @Override
         public int statusCode() {
            return status;
         }

         @Override
         public HttpClient.Version version() {
            return version;
         }
      };
   }
}
