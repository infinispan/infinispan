package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.impl.jdk.RestClientJDK.CONTENT_TYPE;

import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import org.infinispan.client.rest.RestResponseInfo;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 15.0
 **/
public class RestResponseInfoJDK implements RestResponseInfo {
   private final HttpResponse.ResponseInfo responseInfo;

   public RestResponseInfoJDK(HttpResponse.ResponseInfo responseInfo) {
      this.responseInfo = responseInfo;
   }

   @Override
   public int status() {
      return responseInfo.statusCode();
   }

   @Override
   public Map<String, List<String>> headers() {
      return responseInfo.headers().map();
   }

   @Override
   public String header(String header) {
      return responseInfo.headers().firstValue(header).orElse(null);
   }

   @Override
   public Protocol protocol() {
      switch (responseInfo.version()) {
         case HTTP_1_1:
            return Protocol.HTTP_11;
         case HTTP_2:
            return Protocol.HTTP_20;
         default:
            throw new IllegalArgumentException(responseInfo.version().name());
      }
   }

   @Override
   public MediaType contentType() {
      return responseInfo.headers().firstValue(CONTENT_TYPE).map(MediaType::fromString).orElse(null);
   }
}
