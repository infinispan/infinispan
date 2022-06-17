package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.CONTAINS_ENTRY_RESPONSE;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implements "contains entry" for multimap as defined by <a
 * href="http://infinispan.org/docs/dev/user_guide/user_guide.html#hot_rod_protocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
@Immutable
public class ContainsEntryMultimapOperation<K> extends AbstractMultimapKeyValueOperation<K, Boolean> {

   public ContainsEntryMultimapOperation(OperationContext operationContext, K key, byte[] keyBytes,
                                         byte[] value, CacheOptions options, boolean supportsDuplicates) {
      super(operationContext, CONTAINS_ENTRY_REQUEST, CONTAINS_ENTRY_RESPONSE, key, keyBytes, value, options, null, supportsDuplicates);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Boolean.FALSE);
      } else {
         complete(buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE);
      }
   }
}
