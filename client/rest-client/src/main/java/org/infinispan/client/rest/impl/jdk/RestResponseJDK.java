package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.impl.jdk.RestClientJDK.AUTHORIZATION;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 15.0
 **/
public class RestResponseJDK<T> implements RestResponse {
   private final HttpResponse<T> response;

   public RestResponseJDK(HttpResponse<T> response) {
      this.response = response;
   }

   @Override
   public int status() {
      return response.statusCode();
   }

   @Override
   public Map<String, List<String>> headers() {
      return response.headers().map();
   }

   @Override
   public String header(String header) {
      return response.headers().firstValue(header).orElse(null);
   }

   @Override
   public String body() {
      Object body = response.body();
      if (body instanceof String) {
         return (String) body;
      } else if (body instanceof byte[]) {
         return new String((byte[]) body, StandardCharsets.UTF_8);
      } else {
         try (InputStream is = (InputStream) body) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   @Override
   public InputStream bodyAsStream() {
      Object body = response.body();
      if (body instanceof InputStream) {
         return (InputStream) body;
      } else if (body instanceof String) {
         return new ByteArrayInputStream(((String) body).getBytes(StandardCharsets.UTF_8));
      } else {
         return new ByteArrayInputStream((byte[]) body);
      }
   }

   @Override
   public byte[] bodyAsByteArray() {
      Object body = response.body();
      if (body instanceof byte[]) {
         return (byte[]) body;
      } else if (body instanceof String) {
         return ((String) body).getBytes(StandardCharsets.UTF_8);
      } else {
         try (InputStream is = (InputStream) body) {
            return is.readAllBytes();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   @Override
   public Protocol protocol() {
      switch (response.version()) {
         case HTTP_1_1:
            return Protocol.HTTP_11;
         case HTTP_2:
            return Protocol.HTTP_20;
         default:
            throw new IllegalArgumentException(response.version().name());
      }
   }

   @Override
   public MediaType contentType() {
      Optional<String> contentType = response.headers().firstValue(CONTENT_TYPE);
      return contentType.map(MediaType::fromString).orElse(null);
   }

   @Override
   public void close() {
   }

   @Override
   public boolean usedAuthentication() {
      return response.request().headers().firstValue(AUTHORIZATION).isPresent();
   }
}
