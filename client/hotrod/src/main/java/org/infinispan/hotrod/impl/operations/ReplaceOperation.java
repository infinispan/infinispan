package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "Replace" operation as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class ReplaceOperation<K, V> extends AbstractKeyValueOperation<K, CacheEntry<K, V>> {

   public ReplaceOperation(OperationContext operationContext, K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      super(operationContext, REPLACE_REQUEST, REPLACE_RESPONSE, key, keyBytes, value, options, dataFormat);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status)) {
         statsDataStore();
      }
      if (HotRodConstants.hasPrevious(status)) {
         statsDataRead(true);
      }
      complete(returnPossiblePrevValue(buf, status));
   }
}
