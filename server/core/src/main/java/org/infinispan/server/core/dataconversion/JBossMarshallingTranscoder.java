package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.convertTextToObject;
import static org.infinispan.commons.dataconversion.StandardConversions.decodeObjectContent;
import static org.infinispan.commons.dataconversion.StandardConversions.decodeOctetStream;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transcode between application/x-jboss-marshalling and commons formats
 *
 * @since 9.2
 */
public class JBossMarshallingTranscoder extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(JBossMarshallingTranscoder.class, Log.class);
   private final Transcoder jsonObjectTranscoder;
   private final GenericJBossMarshaller marshaller;

   public JBossMarshallingTranscoder(JsonTranscoder jsonObjectTranscoder, GenericJBossMarshaller marshaller) {
      super(APPLICATION_JBOSS_MARSHALLING, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_OBJECT, APPLICATION_JSON, APPLICATION_UNKNOWN);
      this.jsonObjectTranscoder = jsonObjectTranscoder;
      this.marshaller = marshaller;
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(MediaType.APPLICATION_JBOSS_MARSHALLING)) {
         Object decoded = content;
         if (contentType.match(APPLICATION_OBJECT)) {
            decoded = decodeObjectContent(content, contentType);
         }
         if (contentType.match(APPLICATION_OCTET_STREAM)) {
            decoded = decodeOctetStream(content, destinationType);
         }
         if (contentType.match(TEXT_PLAIN)) {
            decoded = convertTextToObject(content, contentType);
         }
         if (contentType.match(APPLICATION_JSON)) {
            decoded = jsonObjectTranscoder.transcode(content, contentType, MediaType.APPLICATION_OBJECT);
         }
         if (contentType.match(APPLICATION_UNKNOWN)) {
            return content;
         }
         return marshall(decoded);
      }
      if (destinationType.match(MediaType.APPLICATION_OCTET_STREAM)) {
         try {
            Object unmarshalled = unmarshall(content);
            if (unmarshalled instanceof byte[]) {
               return unmarshalled;
            }
            return StandardConversions.convertJavaToOctetStream(unmarshalled, MediaType.APPLICATION_OBJECT, marshaller);
         } catch (IOException | InterruptedException e) {
            throw logger.unsupportedContent(content);
         }
      }
      if (destinationType.match(MediaType.TEXT_PLAIN)) {
         String unmarshalled = unmarshall(content).toString();
         return unmarshalled.getBytes(destinationType.getCharset());
      }
      if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
         return unmarshall(content);
      }
      if (destinationType.match(MediaType.APPLICATION_JSON)) {
         // A more efficient way would be to read the jboss marshalling binary payload and convert it directly to json
         // For now it will unmarshall as Java Object first.
         Object unmarshalled = unmarshall(content);
         Object result = jsonObjectTranscoder.transcode(unmarshalled, MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JSON);
         return StandardConversions.convertTextToOctetStream(result, MediaType.APPLICATION_JSON);
      }
      if (destinationType.equals(APPLICATION_UNKNOWN)) {
         try {
            //TODO: Remove wrapping of byte[] into WrappedByteArray from the Hot Rod Multimap operations.
            if (content instanceof WrappedByteArray) return content;
            return StandardConversions.convertJavaToOctetStream(content, MediaType.APPLICATION_OBJECT, marshaller);
         } catch (IOException | InterruptedException e) {
            throw logger.unsupportedContent(content);
         }
      }

      throw logger.unsupportedContent(content);
   }

   private byte[] marshall(Object o) {
      try {
         return marshaller.objectToByteBuffer(o);
      } catch (InterruptedException | IOException e) {
         throw logger.errorTranscoding(e);
      }
   }

   private Object unmarshall(Object o) {
      try {
         return o instanceof byte[] ? marshaller.objectFromByteBuffer((byte[]) o) : o;
      } catch (IOException | ClassNotFoundException e) {
         throw logger.errorTranscoding(e);
      }
   }

}
