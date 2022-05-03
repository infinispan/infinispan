package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_RESPONSE;

import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.exceptions.InvalidResponseException;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "put" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
@Immutable
public class PutKeyValueMultimapOperation<K> extends AbstractKeyValueOperation<K, Void> {

   public PutKeyValueMultimapOperation(OperationContext operationContext,
                                       K key, byte[] keyBytes,
                                       byte[] value, CacheWriteOptions options,
                                       DataFormat dataFormat) {
      super(operationContext, PUT_MULTIMAP_REQUEST, PUT_MULTIMAP_RESPONSE, key, keyBytes, value, options, dataFormat);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (!HotRodConstants.isSuccess(status)) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      complete(null);
   }
}
