package org.infinispan.client.rest.impl.okhttp;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;

import okhttp3.RequestBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class StringRestEntityOkHttp implements RestEntity, RestEntityAdaptorOkHttp {
   private final MediaType contentType;
   private final String body;

   public StringRestEntityOkHttp(MediaType contentType, String body) {
      this.contentType = contentType;
      this.body = body;
   }

   @Override
   public String getBody() {
      return body;
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
