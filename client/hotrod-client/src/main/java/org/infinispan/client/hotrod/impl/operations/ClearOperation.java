package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Corresponds to clear operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class ClearOperation extends AbstractCacheOperation<Void> {

   public ClearOperation(InternalRemoteCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return null;
   }

   @Override
   public short requestOpCode() {
      return CLEAR_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return CLEAR_RESPONSE;
   }
}
