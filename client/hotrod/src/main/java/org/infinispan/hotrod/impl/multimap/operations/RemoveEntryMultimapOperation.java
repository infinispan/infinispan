package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.REMOVE_ENTRY_MULTIMAP_RESPONSE;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.operations.AbstractKeyValueOperation;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "remove" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
@Immutable
public class RemoveEntryMultimapOperation<K> extends AbstractKeyValueOperation<K, Boolean> {

   public RemoveEntryMultimapOperation(OperationContext operationContext, K key, byte[] keyBytes, byte[] value, CacheOptions options) {
      super(operationContext, REMOVE_ENTRY_MULTIMAP_REQUEST, REMOVE_ENTRY_MULTIMAP_RESPONSE, key, keyBytes, value, options, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendKeyValueOperation(channel);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Boolean.FALSE);
      } else {
         complete(buf.readByte() == 1 ? Boolean.TRUE : Boolean.FALSE);
      }
   }
}
