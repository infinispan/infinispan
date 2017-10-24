package org.infinispan.query.remote.impl.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.logging.Log;


/**
 * @since 9.2
 */
public class ProtostreamJsonTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(ProtostreamJsonTranscoder.class, Log.class);

   private final Set<MediaType> supportedTypes;
   private final SerializationContext ctx;

   public ProtostreamJsonTranscoder(SerializationContext ctx) {
      this.ctx = ctx;
      supportedTypes = new HashSet<>();
      supportedTypes.add(APPLICATION_JSON);
      supportedTypes.add(APPLICATION_PROTOSTREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_JSON)) {
            return ProtobufUtil.toCanonicalJSON(ctx, (byte[]) content);
         }
         if (destinationType.match(APPLICATION_PROTOSTREAM)) {
            Reader reader = new InputStreamReader(new ByteArrayInputStream((byte[]) content));
            return ProtobufUtil.fromCanonicalJSON(ctx, reader);
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
