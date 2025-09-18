package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_RESPONSE;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "remove" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class RemoveEntryMultimapOperation extends AbstractMultimapKeyValueOperation<Boolean> {

   public RemoveEntryMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, byte[] value,
                                       boolean supportsDuplicates) {
      super(remoteCache, keyBytes, value, -1, TimeUnit.MILLISECONDS, -1, TimeUnit.MILLISECONDS, supportsDuplicates);
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
      return REMOVE_ENTRY_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_ENTRY_MULTIMAP_RESPONSE;
   }
}
