package org.infinispan.query.remote.impl.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.logging.Log;

public class ProtostreamObjectTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(ProtostreamObjectTranscoder.class, Log.class);

   private final Set<MediaType> supportedTypes;
   private final SerializationContext ctx;

   public ProtostreamObjectTranscoder(SerializationContext ctx) {
      this.ctx = ctx;
      supportedTypes = new HashSet<>();
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_PROTOSTREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OBJECT)) {
            return ProtobufUtil.fromWrappedByteArray(ctx, (byte[]) content);
         }
         if (destinationType.match(APPLICATION_PROTOSTREAM)) {
            return ProtobufUtil.toWrappedByteArray(ctx, content);
         }
      } catch (IOException e) {
         throw log.errorTranscoding(e);
      }
      throw log.unsupportedContent(content);
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
