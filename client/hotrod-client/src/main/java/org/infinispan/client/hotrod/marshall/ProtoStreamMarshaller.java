package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.protostream.SerializationContext;

/**
 * A client-side marshaller that uses Protocol Buffers.
 *
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated since 10.0, will be removed in the future. org.infinispan.commons.marshall.ProtoStreamMarshaller
 * should be used instead.
 */
@Deprecated
public class ProtoStreamMarshaller extends org.infinispan.commons.marshall.ProtoStreamMarshaller {

   /**
    * Obtains the {@link SerializationContext} associated with the given remote cache manager.
    *
    * @param remoteCacheManager the remote cache manager (must not be {@code null})
    * @return the associated {@link SerializationContext}
    * @throws HotRodClientException if the cache manager is not configured to use a {@link org.infinispan.commons.marshall.ProtoStreamMarshaller}
    * @deprecated since 10.0 and will be removed in the future. Use {@link MarshallerUtil#getSerializationContext(RemoteCacheManager)}
    * instead.
    */
   @Deprecated
   public static SerializationContext getSerializationContext(RemoteCacheManager remoteCacheManager) {
      return MarshallerUtil.getSerializationContext(remoteCacheManager);
   }
}
