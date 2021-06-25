package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.JavaStringCodec.BYTE_ARRAY;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

/**
 * Handle conversions between text/plain, url-encoded, java objects, and octet-stream contents.
 *
 * @since 9.2
 */
public final class DefaultTranscoder extends AbstractTranscoder {

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   private final Marshaller marshaller;

   public DefaultTranscoder(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   static {
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
      supportedTypes.add(APPLICATION_WWW_FORM_URLENCODED);
      supportedTypes.add(TEXT_PLAIN);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OCTET_STREAM)) {
            return convertToOctetStream(content, contentType, destinationType);
         }
         if (destinationType.match(APPLICATION_OBJECT)) {
            return convertToObject(content, contentType, destinationType);
         }
         if (destinationType.match(TEXT_PLAIN)) {
            return convertToTextPlain(content, contentType, destinationType);
         }
         if (destinationType.match(APPLICATION_WWW_FORM_URLENCODED)) {
            return content;
         }
         throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
      } catch (EncodingException | InterruptedException | IOException | ClassNotFoundException e) {
         throw CONTAINER.errorTranscoding(Util.toStr(content), contentType, destinationType, e);
      }
   }

   private Object convertToTextPlain(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         return StandardConversions.convertCharset(content, UTF_8, destinationType.getCharset());
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         if (content instanceof byte[]) {
            return StandardConversions.convertCharset(content, StandardCharsets.UTF_8, destinationType.getCharset());
         } else {
            return content.toString().getBytes(destinationType.getCharset());
         }
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToText(content, contentType, destinationType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return content;
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

   private Object convertToObject(Object content, MediaType contentType, MediaType destinationType) throws IOException, ClassNotFoundException {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         String classType = destinationType.getClassType();
         if (classType != null && !(classType.startsWith("java.lang") || classType.equals(BYTE_ARRAY.getName()))) {
            Object unmarshalled = marshaller.objectFromByteBuffer((byte[]) content);
            if (unmarshalled.getClass().getName().equals(classType)) {
               return unmarshalled;
            }
         } else {
            return content;
         }
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return content;
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToObject(content, contentType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return content instanceof byte[] ? new String((byte[]) content, destinationType.getCharset()) : content.toString();
      }
      throw CONTAINER.unsupportedConversion(Util.toStr(content), contentType, destinationType);
   }

   public Object convertToOctetStream(Object content, MediaType contentType, MediaType destinationType) throws IOException, InterruptedException {
      if (contentType.match(APPLICATION_OBJECT)) {
         if (content instanceof byte[]) return content;
         if (content instanceof String) {
            return content.toString().getBytes(UTF_8);
         }
         return marshaller.objectToByteBuffer(content);
      }
      return content;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }

   private boolean in(MediaType mediaType, Set<MediaType> set) {
      return set.stream().anyMatch(s -> s.match(mediaType));
   }

   @Override
   public boolean supportsConversion(MediaType mediaType, MediaType other) {
      return in(mediaType, supportedTypes) && in(other, supportedTypes);
   }
}
