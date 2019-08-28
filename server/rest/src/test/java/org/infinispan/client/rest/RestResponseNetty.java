package org.infinispan.client.rest;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.dataconversion.MediaType;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestResponseNetty implements RestResponse {
   FullHttpResponse response;

   RestResponseNetty(FullHttpResponse response) {
      this.response = response;
   }

   @Override
   public int getStatus() {
      return response.status().code();
   }

   @Override
   public String getBody() {
      return response.content().toString(StandardCharsets.UTF_8);
   }

   @Override
   public MediaType contentType() {
      return MediaType.fromString(response.headers().get(HttpHeaderNames.CONTENT_TYPE));
   }
}
