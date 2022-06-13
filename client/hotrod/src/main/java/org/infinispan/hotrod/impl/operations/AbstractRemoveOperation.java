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
public abstract class AbstractRemoveOperation<K, T> extends AbstractKeyOperation<K, T> {

   public AbstractRemoveOperation(OperationContext operationContext,
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
      if (HotRodConstants.isNotExist(status)) {
         completeNotExist();
      } else {
         completeExisted(buf, status);
      }
   }

   abstract void completeNotExist();

   abstract void completeExisted(ByteBuf buf, short status);
}
