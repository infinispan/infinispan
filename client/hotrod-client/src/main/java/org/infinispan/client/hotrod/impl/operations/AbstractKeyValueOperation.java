package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.InternalFlag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation<T> extends AbstractKeyOperation<T> {
   private static final InternalFlag[] LIFESPAN_NANOS = new InternalFlag[] {InternalFlag.LIFESPAN_NANOS};
   private static final InternalFlag[] MAXIDLE_NANOS = new InternalFlag[] {InternalFlag.MAXIDLE_NANOS};
   private static final InternalFlag[] LIFESPAN_AND_MAXIDLE_NANOS = new InternalFlag[] {InternalFlag.LIFESPAN_NANOS, InternalFlag.MAXIDLE_NANOS};

   protected final byte[] value;

   protected final int lifespan;

   protected final int maxIdle;

   protected final int lifespanNanos;

   protected final int maxIdleNanos;

   protected AbstractKeyValueOperation(Codec codec, TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                       AtomicInteger topologyId, Flag[] flags, byte[] value,
                                       int lifespan, int lifespanNanos,
                                       int maxIdle, int maxIdleNanos) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, internalFlags(lifespanNanos, maxIdleNanos));
      this.value = value;
      this.lifespan = lifespan;
      this.lifespanNanos = lifespanNanos;
      this.maxIdle = maxIdle;
      this.maxIdleNanos = maxIdleNanos;
   }

   private static InternalFlag[] internalFlags(int lifespanNanos, int maxIdleNanos) {
      InternalFlag[] result = null;
      if (lifespanNanos > 0) {
         if (maxIdleNanos > 0) {
            result = LIFESPAN_AND_MAXIDLE_NANOS;
         } else {
            result = LIFESPAN_NANOS;
         }
      } else if (maxIdleNanos > 0) {
         result = MAXIDLE_NANOS;
      }
      return result;
   }

   //[header][key length][key][lifespan][max idle][(optional) lifespan nanos][(optional) max idle nanos][value length][value]
   protected short sendPutOperation(Transport transport, short opCode, byte opRespCode) {
      // 1) write header
      HeaderParams params = writeHeader(transport, opCode);

      // 2) write key and value
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      codec.writeExpirationNanoTimes(transport, lifespanNanos, maxIdleNanos, internalFlags);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, params);
   }
}
