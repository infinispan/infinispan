package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * Handle conversions for the generic binary format 'application/unknown' that is assumed when no MediaType is specified.
 *
 * @since 10.0
 */
public final class BinaryTranscoder extends OneToManyTranscoder {

   private final AtomicReference<Marshaller> marshallerRef;

   public BinaryTranscoder(Marshaller marshaller) {
      super(APPLICATION_UNKNOWN, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, APPLICATION_WWW_FORM_URLENCODED, TEXT_PLAIN);
      this.marshallerRef = new AtomicReference<>(marshaller);
   }

   public void overrideMarshaller(Marshaller marshaller) {
      marshallerRef.set(marshaller);
   }

   private Marshaller getMashaller() {
      return marshallerRef.get();
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.equals(APPLICATION_UNKNOWN)) {
            if (contentType.match(APPLICATION_UNKNOWN)) return content;
            return convertToByteArray(content, contentType);
         }
         if (destinationType.match(APPLICATION_OCTET_STREAM)) {
            return StandardConversions.decodeOctetStream(content, contentType);
         }
         if (destinationType.match(APPLICATION_OBJECT)) {
            if (content instanceof byte[]) {
               return getMashaller().objectFromByteBuffer((byte[]) content);
            }
            if (content instanceof WrappedByteArray) {
               return getMashaller().objectFromByteBuffer(((WrappedByteArray) content).getBytes());
            }
            return content;
         }
         if (destinationType.match(TEXT_PLAIN)) {
            return StandardConversions.convertJavaToOctetStream(content, APPLICATION_OBJECT, getMashaller());
         }
         if (destinationType.match(APPLICATION_WWW_FORM_URLENCODED)) {
            return convertToUrlEncoded(content, contentType);
         }
         throw CONTAINER.unsupportedContent(content);
      } catch (InterruptedException | IOException | EncodingException | ClassNotFoundException e) {
         throw CONTAINER.unsupportedContent(content);
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
         return StandardConversions.convertJavaToOctetStream(content, contentType, getMashaller());
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

}
