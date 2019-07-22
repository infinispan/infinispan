package org.infinispan.commons.marshall;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.infinispan.commons.dataconversion.MediaType;

public final class UTF8StringMarshaller extends StringMarshaller {

   private static final MediaType UTF8_MEDIA_TYPE = MediaType.fromString("text/plain; charset=UTF-8");

   public UTF8StringMarshaller() {
      super(UTF_8);
   }

   @Override
   public MediaType mediaType() {
      return UTF8_MEDIA_TYPE;
   }
}
