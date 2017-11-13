package org.infinispan.client.hotrod.impl.protocol;


import static org.infinispan.client.hotrod.impl.TimeUnitParam.encodeTimeUnits;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

public class Codec22 extends Codec21 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_22);
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      byte timeUnits = encodeTimeUnits(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      buf.writeByte(timeUnits);
      if (lifespan > 0) {
         ByteBufUtil.writeVLong(buf, lifespan);
      }
      if (maxIdle > 0) {
         ByteBufUtil.writeVLong(buf, maxIdle);
      }
   }

   @Override
   public int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return 1 + (lifespan > 0 ? ByteBufUtil.estimateVLongSize(lifespan) : 0) + (lifespan > 0 ? ByteBufUtil.estimateVLongSize(maxIdle) : 0);
   }
}
