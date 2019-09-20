package org.infinispan.server.core.dataconversion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.util.logging.Log;

/**
 * A pass-through transcoder between protostream and unknown format.
 *
 * @since 9.3
 */
public class ProtostreamBinaryTranscoder extends OneToManyTranscoder {

   private static final Log log = LogFactory.getLog(ProtostreamBinaryTranscoder.class, Log.class);

   private final Set<MediaType> supportedTypes = new HashSet<>();
   private final SerializationContext ctx;

   public ProtostreamBinaryTranscoder(SerializationContext ctx) {
      super(MediaType.APPLICATION_PROTOSTREAM, MediaType.APPLICATION_UNKNOWN, MediaType.APPLICATION_OCTET_STREAM);
      this.ctx = ctx;
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (content instanceof byte[]) {
         return content;
      } else if (content instanceof WrappedByteArray) {
         try {
            return ProtobufUtil.toWrappedByteArray(ctx, content);
         } catch (IOException e) {
            throw log.unsupportedContent(content);
         }
      }
      throw log.unsupportedContent(content);
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
