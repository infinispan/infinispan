package org.infinispan.client.hotrod.impl.operations;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedMetadataImpl;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class GetStreamStartOperation extends AbstractKeyOperation<GetStreamStartResponse> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final int batchSize;

   protected GetStreamStartOperation(InternalRemoteCache<?, ?> internalRemoteCache, byte[] keyBytes, int batchSize) {
      super(internalRemoteCache, keyBytes);
      this.batchSize = batchSize;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      ByteBufUtil.writeVInt(buf, batchSize);
   }

   @Override
   public GetStreamStartResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status)) {
         int id = buf.readInt();
         boolean complete = buf.readBoolean();

         short flags = buf.readUnsignedByte();
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
            creation = buf.readLong();
            lifespan = ByteBufUtil.readVInt(buf);
         }
         if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
            lastUsed = buf.readLong();
            maxIdle = ByteBufUtil.readVInt(buf);
         }
         long version = buf.readLong();

         VersionedMetadataImpl versionedMetadata = new VersionedMetadataImpl(creation, lifespan, lastUsed, maxIdle, version);

         int length = ByteBufUtil.readVInt(buf);
         ByteBuf value = buf.readRetainedSlice(length);

         return new GetStreamStartResponse(id, complete, value, versionedMetadata, decoder.getChannel());
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.START_GET_STREAM_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.START_GET_STREAM_RESPONSE;
   }

   @Override
   public void handleDelayedResponse(GetStreamStartResponse responseValue, Channel channel) {
      if (responseValue != null) {
         if (responseValue.value() != null) {
            responseValue.value().release();
         }
         if (!responseValue.complete()) {
            OperationDispatcher dispatcher = internalRemoteCache.getDispatcher();
            SocketAddress socketAddress = ChannelRecord.of(channel != null ? channel : responseValue.channel());
            if (socketAddress != null) {
               if (log.isDebugEnabled()) {
                  log.debugf("Closing get stream %d after response was received following operation completion",
                        responseValue.id());
               }
               HotRodOperation<Void> endOp = internalRemoteCache.getOperationsFactory()
                     .newGetStreamEndOperation(responseValue.id());
               dispatcher.executeOnSingleAddress(endOp, socketAddress);
            } else {
               if (log.isTraceEnabled()) {
                  log.tracef("Unable to close get stream %d as no dispatcher available, assuming server is gone",
                        responseValue.id());
               }
            }
         }
      }
   }
}
