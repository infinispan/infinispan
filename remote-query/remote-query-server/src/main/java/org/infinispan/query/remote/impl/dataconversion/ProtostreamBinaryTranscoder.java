package org.infinispan.query.remote.impl.dataconversion;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.util.logging.Log;

/**
 * A pass-through transcoder between protostream and unknown format.
 *
 * @since 9.3
 */
public class ProtostreamBinaryTranscoder extends OneToManyTranscoder {

   private static final Log log = LogFactory.getLog(ProtostreamBinaryTranscoder.class, Log.class);

   private final Set<MediaType> supportedTypes = new HashSet<>();

   public ProtostreamBinaryTranscoder() {
      super(MediaType.APPLICATION_PROTOSTREAM, MediaType.APPLICATION_UNKNOWN, MediaType.APPLICATION_OCTET_STREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (!(content instanceof byte[])) {
         throw log.unsupportedContent(content);
      }
      return content;
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
