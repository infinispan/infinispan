package org.infinispan.client.hotrod.impl.operations;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class PutStreamStartOperation extends AbstractKeyOperation<PutStreamResponse> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   public static final long VERSION_PUT = 0;
   public static final long VERSION_PUT_IF_ABSENT = -1;
   private final long version;

   private final long lifespan;
   private final TimeUnit lifespanUnit;
   private final long maxIdle;
   private final TimeUnit maxIdleUnit;

   protected PutStreamStartOperation(InternalRemoteCache<?, ?> internalRemoteCache, byte[] keyBytes, long version,
                                     long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      super(internalRemoteCache, keyBytes);
      this.version = version;
      this.lifespan = lifespan;
      this.lifespanUnit = lifespanUnit;
      this.maxIdle = maxIdle;
      this.maxIdleUnit = maxIdleUnit;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      super.writeOperationRequest(channel, buf, codec);
      codec.writeExpirationParams(buf, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      buf.writeLong(version);
   }

   @Override
   public PutStreamResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return new PutStreamResponse(buf.readInt(), decoder.getChannel());
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.START_PUT_STREAM_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.START_PUT_STREAM_RESPONSE;
   }

   @Override
   public void handleDelayedResponse(PutStreamResponse responseValue, Channel channel) {
      if (responseValue != null) {
         if (log.isDebugEnabled()) {
            log.debugf("Closing put stream %d after response was received following operation completion",
                  responseValue.id());
         }
         HotRodOperation<Void> endOp = internalRemoteCache.getOperationsFactory()
               .newPutStreamEndOperation(responseValue.id());
         OperationDispatcher dispatcher = internalRemoteCache.getDispatcher();
         SocketAddress socketAddress = ChannelRecord.of(channel != null ? channel : responseValue.channel());
         if (socketAddress != null) {
            dispatcher.executeOnSingleAddress(endOp, socketAddress);
         } else {
            dispatcher.execute(endOp);
         }
      }
   }
}
