package org.infinispan.query.remote.impl.dataconversion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.logging.Log;

/**
 *
 */
public class ProtostreamTextTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(ProtostreamTextTranscoder.class, Log.class);

   private final Set<MediaType> supported;
   private final SerializationContext ctx;

   public ProtostreamTextTranscoder(SerializationContext ctx) {
      supported = new HashSet<>();
      supported.add(MediaType.APPLICATION_PROTOSTREAM);
      supported.add(MediaType.TEXT_PLAIN);
      this.ctx = ctx;
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(MediaType.APPLICATION_PROTOSTREAM)) {
            return ProtobufUtil.toWrappedByteArray(ctx, content);
         }
         if (destinationType.match(MediaType.TEXT_PLAIN)) {
            return ProtobufUtil.fromWrappedByteArray(ctx, (byte[]) content);
         }
      } catch (IOException e) {
         throw log.errorTranscoding(e);
      }
      throw log.unsupportedContent(content);
   }


   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supported;
   }
}
