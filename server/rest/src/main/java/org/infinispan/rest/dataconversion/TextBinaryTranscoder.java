package org.infinispan.rest.dataconversion;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.2
 */
public class TextBinaryTranscoder implements Transcoder {

   protected final static Log logger = LogFactory.getLog(TextObjectTranscoder.class, Log.class);

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public TextBinaryTranscoder() {
      supportedTypes.add(TEXT_PLAIN);
      supportedTypes.add(APPLICATION_OCTET_STREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_OCTET_STREAM)) {
         if (content instanceof String) {
            return ((String) content).getBytes(UTF_8);
         }
         if (content instanceof byte[]) {
            return content;
         }
         throw new EncodingException("Invalid format for text " + content);
      }
      if (destinationType.match(TEXT_PLAIN)) {
         if (!(content instanceof byte[])) {
            throw new EncodingException("Invalid binary format for  " + content);
         }
         return Base64.getEncoder().encodeToString((byte[]) content);
      }
      return null;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
