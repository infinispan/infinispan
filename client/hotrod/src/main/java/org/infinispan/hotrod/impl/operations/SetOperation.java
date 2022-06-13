package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;

import io.netty.buffer.ByteBuf;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public class SetOperation<K> extends AbstractPutOperation<K, Void> {

   public SetOperation(OperationContext operationContext, K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      super(operationContext, key, keyBytes, value, options, dataFormat);
   }

   @Override
   void completeResponse(ByteBuf buf, short status) {
      assert returnPossiblePrevValue(buf, status) == null;
      complete(null);
   }
}
