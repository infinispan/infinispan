package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class ContainsKeyOperation<K> extends AbstractKeyOperation<K, Boolean> {

   public ContainsKeyOperation(OperationContext operationContext, K key, byte[] keyBytes, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, CONTAINS_KEY_REQUEST, CONTAINS_KEY_RESPONSE, key, keyBytes, options, dataFormat);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status));
   }
}
