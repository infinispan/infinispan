package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;

public class NoCachePingOperation extends AbstractNoCacheHotRodOperation<PingResponse> {
   private static final Log log = LogFactory.getLog(NoCachePingOperation.class);

   // Needs to be instance field so acceptResponse can be invoked multiple times
   private final PingResponse.Decoder pingDecoder = new PingResponse.Decoder();

   @Override
   public PingResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      pingDecoder.processResponse(null, buf, decoder);
      if (HotRodConstants.isSuccess(status)) {
         PingResponse pingResponse = pingDecoder.build(status);

         decoder.setCodec(pingResponse.getVersion().getCodec());
         return pingResponse;
      } else {
         String hexStatus = Integer.toHexString(status);
         if (log.isTraceEnabled())
            log.tracef("Unknown response status: %s", hexStatus);

         throw new InvalidResponseException("Unexpected response status: " + hexStatus);
      }
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.PING_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.PING_RESPONSE;
   }
}
