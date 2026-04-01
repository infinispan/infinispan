package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

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

   private final Marshaller marshaller;
   private final Function<String, Marshaller> marshallerLookup;

   public DefaultTranscoder(Marshaller marshaller) {
      this(marshaller, null);
   }

   public DefaultTranscoder(Marshaller marshaller, Function<String, Marshaller> marshallerLookup) {
      this.marshaller = marshaller;
      this.marshallerLookup = marshallerLookup;
   }

   static {
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
      supportedTypes.add(APPLICATION_WWW_FORM_URLENCODED);
      supportedTypes.add(TEXT_PLAIN);
   }

   /**
    * Gets the appropriate marshaller for the given media type.
    * If the media type specifies a marshaller parameter and a lookup function is available,
    * uses the named marshaller. Otherwise, uses the default marshaller.
    *
    * @param mediaType the media type which may specify a marshaller
    * @return the marshaller to use
    */
   private Marshaller getMarshallerForMediaType(MediaType mediaType) {
      if (marshallerLookup != null) {
         String marshallerName = mediaType.getMarshaller();
         if (marshallerName != null) {
            Marshaller namedMarshaller = marshallerLookup.apply(marshallerName);
            if (namedMarshaller != null) {
               return namedMarshaller;
            }
         }
      }
      return marshaller;
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
         // Check if content is a SerializedObjectWrapper indicating it must be deserialized
         if (content instanceof org.infinispan.commons.marshall.SerializedObjectWrapper) {
            byte[] bytes = ((org.infinispan.commons.marshall.SerializedObjectWrapper) content).getBytes();
            // Use marshaller specified in contentType (the source), since we're unmarshalling FROM bytes
            Marshaller marshallerToUse = getMarshallerForMediaType(contentType);
            Object unmarshalled = marshallerToUse.objectFromByteBuffer(bytes);
            String classType = destinationType.getClassType();
            if (classType == null || unmarshalled.getClass().getName().equals(classType)) {
               return unmarshalled;
            } else {
               log.tracef("Unmarshalled object %s was not assignable to %s", unmarshalled, classType);
            }
         }
         return content;
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

   public Object convertToOctetStream(Object content, MediaType contentType, MediaType destinationType) throws IOException, InterruptedException, ClassNotFoundException {
      // Handle re-marshalling when both source and destination are octet-stream with different marshallers
      if (contentType.match(APPLICATION_OCTET_STREAM) && destinationType.match(APPLICATION_OCTET_STREAM)) {
         String sourceMarshaller = contentType.getMarshaller();
         String destMarshaller = destinationType.getMarshaller();

         // If both have marshaller parameters and they're different, re-marshal
         if (sourceMarshaller != null && destMarshaller != null && !sourceMarshaller.equals(destMarshaller)) {
            Marshaller sourceMarshallerInstance = getMarshallerForMediaType(contentType);
            Marshaller destMarshallerInstance = getMarshallerForMediaType(destinationType);

            // Handle unwrapping if source is SerializedObjectWrapper
            byte[] sourceBytes;
            if (content instanceof org.infinispan.commons.marshall.SerializedObjectWrapper) {
               sourceBytes = ((org.infinispan.commons.marshall.SerializedObjectWrapper) content).getBytes();
            } else {
               sourceBytes = (byte[]) content;
            }

            log.tracef("Re-marshalling %s bytes from marshaller %s to %s",
                  sourceBytes.length, sourceMarshaller, destMarshaller);

            // Unmarshal with source marshaller
            Object unmarshalled = sourceMarshallerInstance.objectFromByteBuffer(sourceBytes);

            // Re-marshal with destination marshaller
            byte[] remarshalled = destMarshallerInstance.objectToByteBuffer(unmarshalled);
            return new org.infinispan.commons.marshall.SerializedObjectWrapper(remarshalled);
         }
      }

      if (contentType.match(APPLICATION_OBJECT)) {
         if (content instanceof byte[]) return content;
         // Wrap marshalled object in SerializedObjectWrapper to signal it must be deserialized
         // Use marshaller specified in destinationType (the target), since we're marshalling TO bytes
         Marshaller marshallerToUse = getMarshallerForMediaType(destinationType);
         byte[] marshalled = marshallerToUse.objectToByteBuffer(content);
         return new org.infinispan.commons.marshall.SerializedObjectWrapper(marshalled);
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
