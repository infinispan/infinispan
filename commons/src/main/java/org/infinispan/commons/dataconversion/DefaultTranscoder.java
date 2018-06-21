package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * Handle conversions between text/plain, url-encoded, java objects, and octet-stream contents.
 *
 * @since 9.2
 */
public final class DefaultTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(DefaultTranscoder.class);

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   private final GenericJBossMarshaller jbossMarshaller;
   private final JavaSerializationMarshaller javaSerializationMarshaller;

   public DefaultTranscoder(GenericJBossMarshaller marshaller, JavaSerializationMarshaller javaSerializationMarshaller) {
      this.javaSerializationMarshaller = javaSerializationMarshaller;
      this.jbossMarshaller = marshaller;
   }

   public DefaultTranscoder() {
      this.javaSerializationMarshaller = new JavaSerializationMarshaller();
      this.jbossMarshaller = new GenericJBossMarshaller();
   }

   static {
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
      supportedTypes.add(APPLICATION_WWW_FORM_URLENCODED);
      supportedTypes.add(TEXT_PLAIN);
      supportedTypes.add(APPLICATION_UNKNOWN);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.equals(APPLICATION_UNKNOWN)) {
            return convertToByteArray(content, contentType);
         }
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
         throw log.unsupportedContent(content);
      } catch (EncodingException | InterruptedException | IOException e) {
         throw log.unsupportedContent(content);
      }
   }

   private Object convertToByteArray(Object content, MediaType contentType) {
      try {
         if (contentType.match(APPLICATION_OCTET_STREAM)) {
            return StandardConversions.decodeOctetStream(content, contentType);
         }
         if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
            return StandardConversions.convertUrlEncodedToOctetStream(content);
         }
         if (contentType.match(TEXT_PLAIN)) {
            return StandardConversions.convertTextToOctetStream(content, contentType);
         }
         return StandardConversions.convertJavaToOctetStream(content, contentType);
      } catch (EncodingException | InterruptedException | IOException e) {
         throw log.unsupportedContent(content);
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
      throw log.unsupportedContent(content);
   }

   private Object convertToTextPlain(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(APPLICATION_UNKNOWN)) {
         try {
            return StandardConversions.convertJavaToOctetStream(content, APPLICATION_OBJECT);
         } catch (IOException | InterruptedException e) {
            throw log.unsupportedContent(content);
         }
      }
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
      throw log.unsupportedContent(content);
   }

   private Object convertToObject(Object content, MediaType contentType, MediaType destinationType) {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         byte[] decoded = StandardConversions.decodeOctetStream(content, destinationType);
         return StandardConversions.convertOctetStreamToJava(decoded, destinationType);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return content;
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToObject(content, contentType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return StandardConversions.convertUrlEncodedToObject(content);
      }
      if (contentType.equals(MediaType.APPLICATION_UNKNOWN)) {
         if (content instanceof byte[]) {
            return tryDeserialize((byte[]) content);
         }
         if (content instanceof WrappedByteArray) {
            return tryDeserialize(((WrappedByteArray) content).getBytes());
         }
         return content;
      }
      throw log.unsupportedContent(content);
   }

   private Object tryDeserialize(byte[] content) {
      try {
         return jbossMarshaller.objectFromByteBuffer(content);
      } catch (IOException | ClassNotFoundException e1) {
         try {
            return javaSerializationMarshaller.objectFromByteBuffer(content);
         } catch (IOException | ClassNotFoundException e2) {
            throw log.unsupportedContent(content);
         }
      }
   }

   public Object convertToOctetStream(Object content, MediaType contentType, MediaType destinationType) throws IOException, InterruptedException {
      if (contentType.match(APPLICATION_OCTET_STREAM) || contentType.match(APPLICATION_UNKNOWN)) {
         return StandardConversions.decodeOctetStream(content, contentType);
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         return StandardConversions.convertJavaToOctetStream(content, contentType);
      }
      if (contentType.match(TEXT_PLAIN)) {
         return StandardConversions.convertTextToOctetStream(content, destinationType);
      }
      if (contentType.match(APPLICATION_WWW_FORM_URLENCODED)) {
         return StandardConversions.convertUrlEncodedToOctetStream(content);
      }
      throw log.unsupportedContent(content);
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
