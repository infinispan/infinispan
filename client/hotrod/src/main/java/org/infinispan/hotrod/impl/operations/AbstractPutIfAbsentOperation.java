package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;
import org.jboss.logging.BasicLogger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
public abstract class AbstractPutIfAbsentOperation<K, T> extends AbstractKeyValueOperation<K, T> {

   private static final BasicLogger log = LogFactory.getLog(AbstractPutIfAbsentOperation.class);

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
         completeResponse(buf, status);
      } else {
         statsDataStore();
         complete(null);
      }
   }

   abstract void completeResponse(ByteBuf buf, short status);
}
