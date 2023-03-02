package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "get" operation as described by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @since 14.0
 */
public class GetOperation<K, V> extends AbstractKeyOperation<K, V> {

   public GetOperation(OperationContext operationContext,
                       K key, byte[] keyBytes, CacheOptions options,
                       DataFormat dataFormat) {
      super(operationContext, GET_REQUEST, GET_RESPONSE, key, keyBytes, options, dataFormat);
   }

   @Override
   public void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      throw new IllegalStateException("Get operation not called manually.");
   }
}
