package org.infinispan.query.remote.impl.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;


/**
 * @since 9.2
 */
public class ProtostreamJsonTranscoder implements Transcoder {

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
      if (destinationType.match(APPLICATION_JSON)) {
         try {
            return ProtobufUtil.toCanonicalJSON(ctx, (byte[]) content);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      } else {
         if (destinationType.match(APPLICATION_PROTOSTREAM)) {
            try {
               Reader reader = new InputStreamReader(new ByteArrayInputStream((byte[]) content));
               return ProtobufUtil.fromCanonicalJSON(ctx, reader);
            } catch (IOException e) {
               e.printStackTrace();
            }
         }
      }
      throw new IllegalArgumentException("Not supported");
   }

   @Override
   public Set<MediaType> getSupportedMediaTypes() {
      return supportedTypes;
   }
}
