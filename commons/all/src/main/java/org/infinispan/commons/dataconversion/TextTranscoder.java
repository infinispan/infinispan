package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;

import org.infinispan.commons.util.Util;

/**
 * Handle conversions for 'text/plain'
 *
 * @since 16.2
 */
public final class TextTranscoder extends OneToManyTranscoder {

   public static final TextTranscoder INSTANCE = new TextTranscoder();

   private TextTranscoder() {
      super(TEXT_PLAIN, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OCTET_STREAM)) {
            return convertToOctetStream(content, contentType, destinationType);
         }
         if (destinationType.match(TEXT_PLAIN) || destinationType.match(APPLICATION_OBJECT)) {
            return convertToTextPlain(content, contentType, destinationType);
         }
         throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
      } catch (EncodingException | IOException e) {
         throw CONTAINER.errorTranscoding(Util.toStr(content), contentType, destinationType, e);
      }
   }

   private byte[] convertToOctetStream(Object content, MediaType contentType, MediaType destinationType) throws IOException {
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertCharset(content, contentType.getCharset(), UTF_8);
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

   private String convertToTextPlain(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(TEXT_PLAIN)) {
         return (String) content;
      }
      if (contentType.match(APPLICATION_OBJECT) || contentType.match(APPLICATION_OCTET_STREAM)) {
         // convertTextToObject handles both byte[] and String to String
         return StandardConversions.convertTextToObject(content, contentType);
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }
}
