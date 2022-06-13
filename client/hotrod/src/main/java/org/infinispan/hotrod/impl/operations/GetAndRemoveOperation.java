package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;

import io.netty.buffer.ByteBuf;

/**
 * Implement "remove" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class GetAndRemoveOperation<K, V> extends AbstractRemoveOperation<K, CacheEntry<K, V>> {

   public GetAndRemoveOperation(OperationContext operationContext,
         K key, byte[] keyBytes, CacheOptions options,
         DataFormat dataFormat) {
      super(operationContext, key, keyBytes, options, dataFormat);
   }

   @Override
   void completeNotExist() {
      complete(null);
   }

   @Override
   void completeExisted(ByteBuf buf, short status) {
      CacheEntry<K, V> result = returnPossiblePrevValue(buf, status);
      statsDataRemove();
      complete(result); // NO_ERROR_STATUS
   }
}
