package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.util.logging.Log;

public class ProtostreamTextTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(ProtostreamTextTranscoder.class, Log.class);

   private final Set<MediaType> supported;
   private final SerializationContext ctx;

   public ProtostreamTextTranscoder(SerializationContext ctx) {
      supported = new HashSet<>();
      supported.add(APPLICATION_PROTOSTREAM);
      supported.add(TEXT_PLAIN);
      this.ctx = ctx;
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_PROTOSTREAM)) {
            if (contentType.match(TEXT_PLAIN)) {
               String decoded = StandardConversions.convertTextToObject(content, contentType);
               return marshall(decoded);
            }
            return marshall(content);
         }
         if (destinationType.match(TEXT_PLAIN)) {
            String decoded = ProtobufUtil.fromWrappedByteArray(ctx, (byte[]) content);
            return decoded.getBytes(destinationType.getCharset());
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

   private byte[] marshall(Object o) throws IOException {
      return ProtobufUtil.toWrappedByteArray(ctx, o);
   }
}
