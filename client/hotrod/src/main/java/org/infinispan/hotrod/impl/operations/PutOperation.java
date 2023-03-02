package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public class PutOperation<K, V> extends AbstractPutOperation<K, CacheEntry<K, V>> {

   public PutOperation(OperationContext operationContext, K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      super(operationContext, key, keyBytes, value, options, dataFormat);
   }

   @Override
   protected int flags() {
      return super.flags() | PrivateHotRodFlag.FORCE_RETURN_VALUE.getFlagInt();
   }
}
