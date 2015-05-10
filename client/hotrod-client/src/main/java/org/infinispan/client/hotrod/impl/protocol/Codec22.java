package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.impl.TimeUnitParam.encodeTimeUnits;
import org.infinispan.client.hotrod.impl.transport.Transport;

public class Codec22 extends Codec21 {
   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_22);
   }

   @Override
   public void writeExpirationParams(Transport transport, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      byte timeUnits = encodeTimeUnits(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeByte(timeUnits);
      if (lifespan > 0) {
         transport.writeVLong(lifespan);
      }
      if (maxIdle > 0) {
         transport.writeVLong(maxIdle);
      }
   }

}
