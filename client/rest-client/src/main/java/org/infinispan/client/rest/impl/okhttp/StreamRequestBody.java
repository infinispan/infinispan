package org.infinispan.client.rest.impl.okhttp;

import java.io.IOException;
import java.io.InputStream;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

/**
 * Support for {@link java.io.InputStream} to be used in POST and PUT {@link okhttp3.RequestBody}.
 * @since 12.0
 */
public class StreamRequestBody extends RequestBody {
   private final MediaType contentType;
   private final InputStream inputStream;

   public StreamRequestBody(MediaType contentType, InputStream inputStream) {
      this.contentType = contentType;
      this.inputStream = inputStream;
   }

   @Override
   public MediaType contentType() {
      return contentType;
   }

   @Override
   public void writeTo(BufferedSink sink) throws IOException {
      try (Source source = Okio.source(inputStream)) {
         sink.writeAll(source);
      }
   }
}
