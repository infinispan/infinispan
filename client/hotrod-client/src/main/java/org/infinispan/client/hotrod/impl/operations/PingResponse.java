package org.infinispan.client.hotrod.impl.operations;

import java.util.BitSet;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.protocol.CodecUtils;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.dataconversion.MediaType;

import io.netty.buffer.ByteBuf;

public class PingResponse {

   public static final PingResponse EMPTY = new PingResponse(null);

   private final short status;
   private final ProtocolVersion version;
   private final MediaType keyMediaType;
   private final MediaType valueMediaType;
   private final Throwable error;
   private final BitSet serverOps;

   private PingResponse(short status, ProtocolVersion version, MediaType keyMediaType, MediaType valueMediaType, BitSet serverOps) {
      this.status = status;
      this.version = version;
      this.keyMediaType = keyMediaType;
      this.valueMediaType = valueMediaType;
      this.serverOps = serverOps;
      this.error = null;
   }

   PingResponse(Throwable error) {
      this.status = -1;
      this.version = ProtocolVersion.DEFAULT_PROTOCOL_VERSION;
      this.keyMediaType = MediaType.APPLICATION_UNKNOWN;
      this.valueMediaType = MediaType.APPLICATION_UNKNOWN;
      this.serverOps = new BitSet(0xFF);
      this.error = error;
   }

   public short getStatus() {
      return status;
   }

   public boolean isSuccess() {
      return HotRodConstants.isSuccess(status);
   }

   public boolean isObjectStorage() {
      return keyMediaType != null && keyMediaType.match(MediaType.APPLICATION_OBJECT);
   }

   public boolean isFailed() {
      return error != null;
   }

   public boolean isCacheNotFound() {
      return error instanceof HotRodClientException && error.getMessage().contains("CacheNotFoundException");
   }

   public BitSet getServerOps() {
      return serverOps;
   }

   public ProtocolVersion getVersion() {
      return version;
   }

   public MediaType getKeyMediaType() {
      return keyMediaType;
   }

   public MediaType getValueMediaType() {
      return valueMediaType;
   }

   public static class Decoder {
      int decoderState = 0;
      ProtocolVersion serverVersion;
      int serverOpsCount = -1;
      BitSet serverOps;
      int serverOpsSeen = 0; // BitSet.cardinality does a full scan of the bitset, so we use this shortcut
      MediaType keyMediaType;
      MediaType valueMediaType;

      void processResponse(ByteBuf buf, HeaderDecoder decoder) {
         if (decoderState < 4) {
            switch (decoderState) {
               case 0:
                  keyMediaType = CodecUtils.readMediaType(buf);
                  valueMediaType = CodecUtils.readMediaType(buf);
                  decoder.checkpoint();
                  ++decoderState;
               case 1:
                  serverVersion = ProtocolVersion.getBestVersion(buf.readUnsignedByte());
                  decoder.checkpoint();
                  ++decoderState;
               case 2:
                  serverOpsCount = ByteBufUtil.readVInt(buf);
                  serverOps = new BitSet(0xFF);
                  decoder.checkpoint();
                  ++decoderState;
               case 3:
                  while (serverOpsSeen < serverOpsCount) {
                     short opCode = buf.readShort();
                     serverOps.set(opCode);
                     serverOpsSeen++;
                     decoder.checkpoint();
                  }
                  ++decoderState;
            }
         }
      }

      PingResponse build(short status) {
         assert decoderState == 4 : "Invalid decoder state";
         return new PingResponse(status, serverVersion, keyMediaType, valueMediaType, serverOps);
      }

      public void reset() {
         decoderState = 0;
      }
   }
}
