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
import org.infinispan.commons.util.Util;

/**
 * Handle conversions for the generic binary format 'application/unknown' that is assumed when no MediaType is specified.
 *
 * This transcoder will not perform any conversion in the data, except those performed by {@link MediaTypeCodec} and
 * for {@link MediaType#APPLICATION_OBJECT} it will use the default marshaller present in the server.
 *
 * @since 10.0
 * @deprecated since 13.0. Will be removed in a future version together with {@link MediaType#APPLICATION_UNKNOWN}.
 */
@Deprecated
public final class BinaryTranscoder extends OneToManyTranscoder {

   private final AtomicReference<Marshaller> marshallerRef;

   public BinaryTranscoder(Marshaller marshaller) {
      super(APPLICATION_UNKNOWN, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, APPLICATION_WWW_FORM_URLENCODED, TEXT_PLAIN);
      this.marshallerRef = new AtomicReference<>(marshaller);
   }

   public void overrideMarshaller(Marshaller marshaller) {
      marshallerRef.set(marshaller);
   }

   private Marshaller getMarshaller() {
      return marshallerRef.get();
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OBJECT)) {
            return getMarshaller().objectFromByteBuffer((byte[]) content);
         }

         if (contentType.match(APPLICATION_OBJECT)) {
            content = getMarshaller().objectToByteBuffer(content);
         }
         return content;
      } catch (ClassCastException | IOException | EncodingException | ClassNotFoundException | InterruptedException e) {
         throw CONTAINER.errorTranscoding(Util.toStr(content), contentType, destinationType, e);
      }
   }

}
