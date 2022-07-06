package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.jboss.logging.BasicLogger;

import io.netty.buffer.ByteBuf;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class PutIfAbsentOperation<K, V> extends AbstractPutIfAbsentOperation<K, CacheEntry<K, V>> {

   private static final BasicLogger log = LogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(OperationContext operationContext,
         K key, byte[] keyBytes, byte[] value,
         CacheWriteOptions options,
         DataFormat dataFormat) {
      super(operationContext, key, keyBytes, value, options, dataFormat);
   }

   @Override
   void completeResponseExistent(ByteBuf buf, short status) {
      CacheEntry<K, V> prevValue = returnPossiblePrevValue(buf, status);
      if (HotRodConstants.hasPrevious(status)) {
         statsDataRead(true);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Returning from putIfAbsent: %s", prevValue);
      }
      complete(prevValue);
   }

   @Override
   void completeResponseNotExistent(ByteBuf buf, short status) {
      if (log.isTraceEnabled()) {
         log.tracef("Returning from putIfAbsent created new entry");
      }

      statsDataStore();
      complete(null);
   }

   @Override
   protected int flags() {
      return super.flags() | PrivateHotRodFlag.FORCE_RETURN_VALUE.getFlagInt();
   }
}
