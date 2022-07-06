package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public abstract class AbstractPutIfAbsentOperation<K, T> extends AbstractKeyValueOperation<K, T> {

   public AbstractPutIfAbsentOperation(OperationContext operationContext,
         K key, byte[] keyBytes, byte[] value,
         CacheWriteOptions options,
         DataFormat dataFormat) {
      super(operationContext, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE, key, keyBytes, value, options, dataFormat);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExecuted(status)) {
         completeResponseExistent(buf, status);
      } else {
         completeResponseNotExistent(buf, status);
      }
   }

   /**
    * Complete the response on case where the key already has a value associated with.
    *
    * @param buf: the response buffer.
    * @param status: the response status.
    */
   abstract void completeResponseExistent(ByteBuf buf, short status);

   /**
    * Complete the response on case where the key was not associated with a value previously.
    *
    * @param buf: the response buffer.
    * @param status: the response status.
    */
   abstract void completeResponseNotExistent(ByteBuf buf, short status);
}
