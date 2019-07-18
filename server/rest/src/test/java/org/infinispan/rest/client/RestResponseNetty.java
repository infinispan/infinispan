package org.infinispan.rest.client;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestResponseNetty implements RestResponse {
   private final String contentType;
   private final int statusCode;
   private final String body;

   RestResponseNetty(FullHttpResponse response) {
      this.statusCode = response.status().code();
      this.body = response.content().toString(StandardCharsets.UTF_8);
      this.contentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
   }

   @Override
   public int getStatus() {
      return statusCode;
   }

   @Override
   public String getBody() {
      return body;
   }

   @Override
   public MediaType contentType() {
      return MediaType.fromString(contentType);
   }

   @Override
   public Protocol getProtocol() {
      return null;
   }
}
