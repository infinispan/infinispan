package org.infinispan.query.remote;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;

//todo [anistor] having a static holder is not ideal

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class SerializationContextHolder {

   private static SerializationContext serCtx = null;

   public static synchronized SerializationContext getSerializationContext() {
      if (serCtx == null) {
         serCtx = ProtobufUtil.newSerializationContext();
      }
      return serCtx;
   }
}
