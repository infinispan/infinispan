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

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

/**
 * Handle conversions between text/plain, url-encoded, java objects, and octet-stream contents.
 *
 * @since 9.2
 */
public final class DefaultTranscoder extends AbstractTranscoder {

   private static final Log log = LogFactory.getLog(DefaultTranscoder.class);
   private static final Set<MediaType> supportedTypes = new HashSet<>();
   private static final String USE_GLOBAL_MEDIA_TYPE = "useGlobalMarshaller";

   private final Marshaller marshaller;
   private final Marshaller globalMarshaller;

   public DefaultTranscoder(Marshaller marshaller) {
      this(marshaller, null);
   }

   public DefaultTranscoder(Marshaller marshaller, Marshaller globalMarshaller) {
      this.marshaller = marshaller;
      this.globalMarshaller = globalMarshaller;
   }

   static {
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
      supportedTypes.add(APPLICATION_WWW_FORM_URLENCODED);
      supportedTypes.add(TEXT_PLAIN);
   }

   public static MediaType useGlobalMarshaller(MediaType mediaType) {
      return mediaType.withParameter(USE_GLOBAL_MEDIA_TYPE, "true");
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

   private Object deserialize(byte[] content, MediaType destinationType) throws IOException, ClassNotFoundException {
      if (destinationType.getParameter(USE_GLOBAL_MEDIA_TYPE).stream().anyMatch(Boolean::parseBoolean)) {
         return globalMarshaller.objectFromByteBuffer(content);
      }
      return marshaller.objectFromByteBuffer(content);
   }

   private byte[] serialize(Object obj, MediaType sourcetype) throws IOException, InterruptedException {
      if (sourcetype.getParameter(USE_GLOBAL_MEDIA_TYPE).stream().anyMatch(Boolean::parseBoolean)) {
         return globalMarshaller.objectToByteBuffer(obj);
      }
      return marshaller.objectToByteBuffer(obj);
   }

   private Object convertToObject(Object content, MediaType contentType, MediaType destinationType) throws IOException, ClassNotFoundException {
      if (contentType.match(APPLICATION_OCTET_STREAM)) {
         String classType = destinationType.getClassType();
         if (classType != null && !(classType.startsWith("java.lang") || classType.equals(BYTE_ARRAY.getName()))) {
            Object unmarshalled = deserialize((byte[]) content, destinationType);
            if (!Class.forName(classType).isAssignableFrom(unmarshalled.getClass())) {
               throw new ClassCastException("Decoded object for class + " + unmarshalled.getClass() + " is not assignable from " + classType);
            }
            return unmarshalled;
         } else {
            // We don't try to unmarshall an object from octet-stream unless the object storage defines a type class
            return content;
         }
      }
      if (contentType.match(APPLICATION_OBJECT)) {
         if (content instanceof byte[] && destinationType.getClassType() != null && contentType.getClassType() == null) {
            content = deserialize((byte[]) content, destinationType);
         }
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
         return serialize(content, contentType);
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
