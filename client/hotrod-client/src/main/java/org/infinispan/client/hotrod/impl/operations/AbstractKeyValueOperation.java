package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import net.jcip.annotations.Immutable;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation<T> extends AbstractKeyOperation<T> {

   protected final byte[] value;

   protected final long lifespan;

   protected final long maxIdle;

   protected final TimeUnit lifespanTimeUnit;

   protected final TimeUnit maxIdleTimeUnit;

   protected AbstractKeyValueOperation(Codec codec, TransportFactory transportFactory, Object key, byte[] keyBytes, byte[] cacheName,
                                       AtomicInteger topologyId, int flags, Configuration cfg, byte[] value,
                                       long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      super(codec, transportFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
   }

   //[header][key length][key][lifespan][max idle][value length][value]
   protected short sendPutOperation(Transport transport, short opCode, byte opRespCode) {
      // 1) write header
      HeaderParams params = writeHeader(transport, opCode);

      // 2) write key and value
      transport.writeArray(keyBytes);
      codec.writeExpirationParams(transport, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, params);
   }
}
