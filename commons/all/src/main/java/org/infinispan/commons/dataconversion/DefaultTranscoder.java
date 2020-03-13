package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.marshall.Marshaller;

/**
 * Handle conversions between text/plain, url-encoded, java objects, and octet-stream contents.
 *
 * @since 9.2
 */
public final class DefaultTranscoder implements Transcoder {

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
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
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
            return convertToUrlEncoded(content, contentType);
         }
         throw CONTAINER.unsupportedContent(content);
      } catch (EncodingException | InterruptedException | IOException e) {
         throw CONTAINER.unsupportedContent(content);
      }
   }

   private Object convertToUrlEncoded(Object content, MediaType contentType) {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         return StandardConversions.convertOctetStreamToUrlEncoded(content, contentType);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return StandardConversions.convertUrlEncodedToObject(content);
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToUrlEncoded(content, contentType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return content;
      }
      throw CONTAINER.unsupportedContent(content);
   }

   private Object convertToTextPlain(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         byte[] decoded = StandardConversions.decodeOctetStream(content, destinationType);
         return StandardConversions.convertOctetStreamToText(decoded, destinationType);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return StandardConversions.convertJavaToText(content, contentType, destinationType);
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToText(content, contentType, destinationType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return StandardConversions.convertUrlEncodedToText(content, destinationType);
      }
      throw CONTAINER.unsupportedContent(content);
   }

   private Object convertToObject(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         byte[] decoded = StandardConversions.decodeOctetStream(content, destinationType);
         return StandardConversions.convertOctetStreamToJava(decoded, destinationType, marshaller);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return StandardConversions.decodeObjectContent(content, contentType);
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToObject(content, contentType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return StandardConversions.convertUrlEncodedToObject(content);
      }
      throw CONTAINER.unsupportedContent(content);
   }

   public Object convertToOctetStream(Object content, MediaType contentType, MediaType destinationType) throws IOException, InterruptedException {
      if (contentType.match(APPLICATION_OCTET_STREAM) || contentType.match(APPLICATION_UNKNOWN)) {
         return StandardConversions.decodeOctetStream(content, contentType);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return StandardConversions.convertJavaToOctetStream(content, contentType, marshaller);
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToOctetStream(content, destinationType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return StandardConversions.convertUrlEncodedToOctetStream(content);
      }
      throw CONTAINER.unsupportedContent(content);
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
