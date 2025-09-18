package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_RESPONSE;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "contains entry" for multimap as defined by <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#hot_rod_protocol">Hot
 * Rod protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class ContainsEntryMultimapOperation extends AbstractMultimapKeyValueOperation<Boolean> {

   public ContainsEntryMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes,
                                         byte[] value, boolean supportsDuplicates) {
      // TODO: this should be refactored in https://issues.redhat.com/browse/ISPN-16469 to not have expiration
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
      return CONTAINS_ENTRY_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return CONTAINS_ENTRY_RESPONSE;
   }
}
