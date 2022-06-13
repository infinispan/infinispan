package org.infinispan.hotrod.impl.operations;

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
public class SetIfAbsentOperation<K> extends AbstractPutIfAbsentOperation<K, Boolean> {

   private static final BasicLogger log = LogFactory.getLog(SetIfAbsentOperation.class);

   public SetIfAbsentOperation(OperationContext operationContext,
         K key, byte[] keyBytes, byte[] value,
         CacheWriteOptions options,
         DataFormat dataFormat) {
      super(operationContext, key, keyBytes, value, options, dataFormat);
   }

   @Override
   void completeResponse(ByteBuf buf, short status) {
      boolean wasSuccess = HotRodConstants.isSuccess(status);
      if (log.isTraceEnabled()) {
         log.tracef("Returning from setIfAbsent: %s", wasSuccess);
      }
      complete(wasSuccess);
   }
}
