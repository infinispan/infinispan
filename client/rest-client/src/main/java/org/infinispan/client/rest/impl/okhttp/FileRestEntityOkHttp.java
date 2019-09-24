package org.infinispan.client.rest.impl.okhttp;

import java.io.File;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.commons.dataconversion.MediaType;

import okhttp3.RequestBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class FileRestEntityOkHttp implements RestEntity, RestEntityAdaptorOkHttp {
   private final MediaType contentType;
   private final File file;

   public FileRestEntityOkHttp(MediaType contentType, File file) {
      this.contentType = contentType;
      this.file = file;
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
      return RequestBody.create(okhttp3.MediaType.get(contentType.toString()), file);
   }
}
