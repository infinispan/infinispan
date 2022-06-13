package org.infinispan.hotrod.impl.operations;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.hotrod.configuration.ProtocolVersion;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

public class PingResponse {

   public static final PingResponse EMPTY = new PingResponse(null);

   private final short status;
   private final ProtocolVersion version;
   private final MediaType keyMediaType;
   private final MediaType valueMediaType;
   private final Throwable error;
   private final Set<Short> serverOps;

   private PingResponse(short status, ProtocolVersion version, MediaType keyMediaType, MediaType valueMediaType, Set<Short> serverOps) {
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
      this.serverOps = Collections.emptySet();
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

   public Set<Short> getServerOps() {
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
      final ProtocolVersion version;
      int decoderState = 0;
      ProtocolVersion serverVersion;
      int serverOpsCount = -1;
      Set<Short> serverOps;
      MediaType keyMediaType;
      MediaType valueMediaType;

      Decoder(ProtocolVersion version) {
         this.version = version;
      }

      void processResponse(Codec codec, ByteBuf buf, HeaderDecoder decoder) {
         while (decoderState < 4) {
            switch (decoderState) {
               case 0:
                  keyMediaType = codec.readKeyType(buf);
                  valueMediaType = codec.readKeyType(buf);
                  decoder.checkpoint();
                  ++decoderState;
               case 1:
                  serverVersion = ProtocolVersion.getBestVersion(buf.readUnsignedByte());
                  decoder.checkpoint();
                  ++decoderState;
               case 2:
                  serverOpsCount = ByteBufUtil.readVInt(buf);
                  serverOps = new TreeSet<>();
                  decoder.checkpoint();
                  ++decoderState;
               case 3:
                  while (serverOps.size() < serverOpsCount) {
                     short opCode = buf.readShort();
                     serverOps.add(opCode);
                     decoder.checkpoint();
                  }
                  ++decoderState;
            }
         }
      }

      PingResponse build(short status) {
         assert decoderState == 4 : "Invalid decoder state";
         return new PingResponse(status, version, keyMediaType, valueMediaType, serverOps);
      }

      public void reset() {
         decoderState = 0;
      }
   }
}
