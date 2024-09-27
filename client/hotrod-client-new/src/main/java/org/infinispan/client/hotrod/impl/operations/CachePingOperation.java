package org.infinispan.client.hotrod.impl.operations;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

import io.netty.handler.codec.DecoderException;

public class CachePingOperation extends NoCachePingOperation {
   private final String cacheName;

   public CachePingOperation(String cacheName) {
      this.cacheName = cacheName;
   }


   @Override
   public short requestOpCode() {
      return HotRodConstants.PING_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.PING_RESPONSE;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return cacheName.getBytes(StandardCharsets.UTF_8);
   }

   @Override
   public boolean supportRetry() {
      // This is actually a cache operation, but we reuse the no cache logic
      return true;
   }

   @Override
   public boolean completeExceptionally(Throwable cause) {
      // This is just how the old code used to be... please feel free to refactor later!
      while (cause instanceof DecoderException && cause.getCause() != null) {
         cause = cause.getCause();
      }
      PingResponse pingResponse = new PingResponse(cause);
      if (pingResponse.isCacheNotFound()) {
         complete(pingResponse);
         return false;
      }
      return super.completeExceptionally(cause);
   }
}
