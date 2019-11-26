package org.infinispan.client.rest.impl.okhttp;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;

import okhttp3.RequestBody;

/**
 * @since 10.1
 **/
public class ByteArrayRestEntityOkHttp implements RestEntity, RestEntityAdaptorOkHttp {
   private final MediaType contentType;
   private final byte[] body;

   public ByteArrayRestEntityOkHttp(MediaType contentType, byte[] body) {
      this.contentType = contentType;
      this.body = body;
   }

   @Override
   public String getBody() {
      return new String(body, StandardCharsets.UTF_8);
   }

   @Override
   public MediaType contentType() {
      return contentType;
   }

   @Override
   public RequestBody toRequestBody() {
      return RequestBody.create(okhttp3.MediaType.get(contentType.toString()), body);
   }
}
