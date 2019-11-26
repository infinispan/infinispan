package org.infinispan.client.rest.impl.okhttp;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;

import okhttp3.Response;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestResponseOkHttp implements RestResponse {
   Response response;

   RestResponseOkHttp(Response response) {
      this.response = response;
   }

   @Override
   public int getStatus() {
      return response.code();
   }

   @Override
   public Map<String, List<String>> headers() {
      return response.headers().toMultimap();
   }

   @Override
   public String getBody() {
      try {
         return response.body().string();
      } catch (IOException e) {
         return null;
      }
   }

   @Override
   public InputStream getBodyAsStream() {
      return response.body().byteStream();
   }

   @Override
   public byte[] getBodyAsByteArray() {
      try {
         return response.body().bytes();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
   }

   @Override
   public MediaType contentType() {
      okhttp3.MediaType mediaType = response.body().contentType();
      return mediaType == null ? null : MediaType.fromString(mediaType.toString());
   }

   @Override
   public Protocol getProtocol() {
      switch (response.protocol()) {
         case H2_PRIOR_KNOWLEDGE:
         case HTTP_2:
            return Protocol.HTTP_20;
         case HTTP_1_1:
            return Protocol.HTTP_11;
         default:
            throw new IllegalStateException("Unknown protocol " + response.protocol());
      }
   }

   @Override
   public void close() {
      response.close();
   }
}
