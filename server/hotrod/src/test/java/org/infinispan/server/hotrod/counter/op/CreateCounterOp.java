package org.infinispan.server.hotrod.counter.op;

import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;
import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_CREATE;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedInt;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.server.hotrod.HotRodOperation;

import io.netty.buffer.ByteBuf;

/**
 * A test {@link HotRodOperation#COUNTER_CREATE} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CreateCounterOp extends CounterOp {

   private final CounterConfiguration configuration;

   public CreateCounterOp(byte version, String counterName, CounterConfiguration configuration) {
      super(version, COUNTER_CREATE, counterName);
      this.configuration = configuration;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      super.writeTo(buffer);
      encodeConfiguration(configuration, buffer::writeByte, buffer::writeLong,
            value -> writeUnsignedInt(value, buffer));
   }
}
