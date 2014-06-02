package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation<T> extends AbstractKeyOperation<T> {

   protected final byte[] value;

   protected final int lifespan;

   protected final int maxIdle;

   protected AbstractKeyValueOperation(Codec codec, TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                       AtomicInteger topologyId, Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
   }

   protected HeaderParams writeKeyValueRequest(Transport transport, byte opCode) {
      HeaderParams params = writeHeader(transport, opCode);
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeArray(value);
      return params;
   }
}
