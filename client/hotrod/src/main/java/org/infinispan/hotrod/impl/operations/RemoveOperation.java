package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implement "remove" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public class RemoveOperation<K, V> extends AbstractKeyOperation<K, V> {

   public RemoveOperation(OperationContext operationContext,
                          K key, byte[] keyBytes, CacheOptions options,
                          DataFormat dataFormat) {
      super(operationContext, REMOVE_REQUEST, REMOVE_RESPONSE, key, keyBytes, options, dataFormat);
   }

   @Override
   public void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      V result = returnPossiblePrevValue(buf, status);
      if (HotRodConstants.isNotExist(status)) {
         complete(null);
      } else {
         statsDataRemove();
         complete(result); // NO_ERROR_STATUS
      }
   }
}
