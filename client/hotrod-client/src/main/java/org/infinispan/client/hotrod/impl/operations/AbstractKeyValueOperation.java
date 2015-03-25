package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.InternalFlag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation<T> extends AbstractKeyOperation<T> {
   private static final long NANOS_IN_SEC = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);

   protected final byte[] value;

   protected final long lifespan;

   protected final long maxIdle;

   protected AbstractKeyValueOperation(Codec codec, TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                       AtomicInteger topologyId, Flag[] flags, byte[] value,
                                       long lifespan, long maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, internalFlags(lifespan, maxIdle));
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
   }

   //[header][key length][key][lifespan][max idle][value length][value]
   protected short sendPutOperation(Transport transport, short opCode, byte opRespCode) {
      // 1) write header
      HeaderParams params = writeHeader(transport, opCode);

      // 2) write key and value
      transport.writeArray(key);
      codec.writeExpirationParams(transport, lifespan, maxIdle, internalFlags);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, params);
   }

   private static InternalFlag[] internalFlags(long lifespan, long maxIdle) {
      if ((lifespan % NANOS_IN_SEC != 0) || (maxIdle % NANOS_IN_SEC != 0)) {
         return new InternalFlag[] {InternalFlag.NANO_DURATIONS};
      } else {
         return null;
      }
   }
}
