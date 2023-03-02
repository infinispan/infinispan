package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "put" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public abstract class AbstractPutOperation<K, T> extends AbstractKeyValueOperation<K, T> {

   public AbstractPutOperation(OperationContext operationContext, K key, byte[] keyBytes, byte[] value, CacheWriteOptions options, DataFormat dataFormat) {
      super(operationContext, PUT_REQUEST, PUT_RESPONSE, key, keyBytes, value, options, dataFormat);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      throw new IllegalStateException("Put operation not called manually.");
   }
}
