package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "containsKey" operation as described in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ContainsKeyOperation extends AbstractKeyOperation<Boolean> {

   public ContainsKeyOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes) {
      super(remoteCache, keyBytes);
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, keyBytes);
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return !HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status);
   }

   @Override
   public short requestOpCode() {
      return CONTAINS_KEY_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return CONTAINS_KEY_RESPONSE;
   }
}
