package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_KEY_MULTIMAP_RESPONSE;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * Implements "remove" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class RemoveKeyMultimapOperation extends AbstractMultimapKeyOperation<Boolean> {
   public RemoveKeyMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, boolean supportsDuplicates) {
      super(remoteCache, keyBytes, supportsDuplicates);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status)) {
         return Boolean.FALSE;
      } else {
         return buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE;
      }
   }

   @Override
   public short requestOpCode() {
      return REMOVE_KEY_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_KEY_MULTIMAP_RESPONSE;
   }
}
