package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Implements "removeIfUnmodified" operation as defined by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class RemoveIfUnmodifiedOperation<V> extends AbstractKeyOperation<VersionedOperationResponse<V>> {

   private final long version;

   public RemoveIfUnmodifiedOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, long version) {
      super(remoteCache, keyBytes);
      this.version = version;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      buf.writeLong(version);
   }

   @Override
   public VersionedOperationResponse<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return returnVersionedOperationResponse(buf, status, codec, unmarshaller);
   }

   @Override
   public short requestOpCode() {
      return REMOVE_IF_UNMODIFIED_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return REMOVE_IF_UNMODIFIED_RESPONSE;
   }
}
