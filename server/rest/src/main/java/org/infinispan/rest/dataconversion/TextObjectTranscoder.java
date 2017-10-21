package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.2
 */
public class TextObjectTranscoder implements Transcoder {

   protected final static Log logger = LogFactory.getLog(TextObjectTranscoder.class, Log.class);

   private static final Set<MediaType> supportedTypes = new HashSet<>();

   public TextObjectTranscoder() {
      supportedTypes.add(TEXT_PLAIN);
      supportedTypes.add(APPLICATION_OBJECT);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(TEXT_PLAIN)) {
         if(content instanceof byte[]) return content;
         return content.toString();
      }
      return content;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
