package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationEndOperation extends AbstractCacheOperation<IterationEndResponse> {
   private final byte[] iterationId;

   protected IterationEndOperation(InternalRemoteCache<?, ?> cache, byte[] iterationId) {
      super(cache);
      this.iterationId = iterationId;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, iterationId);
   }

   @Override
   public IterationEndResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return new IterationEndResponse(status);
   }

   @Override
   public short requestOpCode() {
      return ITERATION_END_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ITERATION_END_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }

   @Override
   public DataFormat getDataFormat() {
      // No data format sent for ending an iteration operation
      return null;
   }
}
