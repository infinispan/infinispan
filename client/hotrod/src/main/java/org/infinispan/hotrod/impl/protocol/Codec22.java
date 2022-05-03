package org.infinispan.hotrod.impl.protocol;


import static org.infinispan.hotrod.impl.TimeUnitParam.encodeTimeUnits;

import java.time.Duration;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

public class Codec22 extends Codec21 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_22);
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, CacheEntryExpiration.Impl expiration) {
      byte timeUnits = encodeTimeUnits(expiration);
      buf.writeByte(timeUnits);
      Duration lifespan = expiration.rawLifespan();
      if (lifespan != null && lifespan != Duration.ZERO) {
         ByteBufUtil.writeVLong(buf, lifespan.toSeconds());
      }
      Duration maxIdle = expiration.rawMaxIdle();
      if (maxIdle != null && lifespan != Duration.ZERO) {
         ByteBufUtil.writeVLong(buf, maxIdle.toSeconds());
      }
   }

   @Override
   public int estimateExpirationSize(CacheEntryExpiration.Impl expiration) {
      int lifespanSeconds = durationToSeconds(expiration.rawLifespan());
      int maxIdleSeconds = durationToSeconds(expiration.rawMaxIdle());
      return 1 + (lifespanSeconds > 0 ? ByteBufUtil.estimateVLongSize(lifespanSeconds) : 0) + (maxIdleSeconds > 0 ? ByteBufUtil.estimateVLongSize(maxIdleSeconds) : 0);
   }

   private int durationToSeconds(Duration duration) {
      return duration == null ? 0 : (int) duration.toSeconds();
   }
}
