package org.infinispan.hotrod.impl.query;

import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.protostream.SerializationContext;

/**
 * @since 14.0
 **/
public class RemoteQuery {
   public SerializationContext getSerializationContext() {
      return null;
   }

   public Object getQueryRequest() {
      return null;
   }

   public RemoteCache<Object, Object> getCache() {
      return null;
   }
}
