package org.infinispan.query.remote.impl.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.util.logging.Log;

public class ProtostreamObjectTranscoder implements Transcoder {

   private static final Log log = LogFactory.getLog(ProtostreamObjectTranscoder.class, Log.class);

   private final Set<MediaType> supportedTypes;
   private final SerializationContext ctx;
   private final ClassLoader classLoader;

   public ProtostreamObjectTranscoder(SerializationContext ctx, ClassLoader classLoader) {
      this.ctx = ctx;
      this.classLoader = classLoader;
      supportedTypes = new HashSet<>();
      supportedTypes.add(APPLICATION_OBJECT);
      supportedTypes.add(APPLICATION_PROTOSTREAM);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(APPLICATION_OBJECT)) {
            Optional<String> type = destinationType.getParameter("type");
            if (!type.isPresent()) {
               return ProtobufUtil.fromWrappedByteArray(ctx, (byte[]) content);
            }
            Class<?> destination = Util.loadClass(type.get(), classLoader);
            byte[] bytes = (byte[]) content;
            return ProtobufUtil.fromByteArray(ctx, bytes, 0, bytes.length, destination);
         }
         if (destinationType.match(APPLICATION_PROTOSTREAM)) {
            Object decoded = StandardConversions.decodeObjectContent(content, contentType);
            Optional<String> wrappedParam = destinationType.getParameter("wrapped");
            if (!wrappedParam.isPresent() || !wrappedParam.get().equals("false"))
               return ProtobufUtil.toWrappedByteArray(ctx, decoded);
            return ProtobufUtil.toByteArray(ctx, decoded);
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
