package org.infinispan.client.rest.impl.okhttp;

import java.io.InputStream;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;

import okhttp3.RequestBody;

/**
 * @since 12.0
 */
public class InputStreamEntityOkHttp implements RestEntity, RestEntityAdaptorOkHttp {
   private final MediaType contentType;
   private final InputStream inputStream;

   public InputStreamEntityOkHttp(MediaType contentType, InputStream inputStream) {
      this.contentType = contentType;
      this.inputStream = inputStream;
   }

   @Override
   public String getBody() {
      throw new UnsupportedOperationException();
   }

   @Override
   public MediaType contentType() {
      return contentType;
   }

   @Override
   public RequestBody toRequestBody() {
      return new StreamRequestBody(okhttp3.MediaType.get(contentType.toString()), inputStream);
   }
}
