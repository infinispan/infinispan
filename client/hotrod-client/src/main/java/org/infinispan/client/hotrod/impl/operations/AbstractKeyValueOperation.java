package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
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
public abstract class AbstractKeyValueOperation extends AbstractKeyOperation {

   protected final byte[] value;

   protected final int lifespan;

   protected final int maxIdle;

   protected AbstractKeyValueOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                    AtomicInteger topologyId, Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(transportFactory, key, cacheName, topologyId, flags);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
   }

   //[header][key length][key][lifespan][max idle][value length][value]
   protected short sendPutOperation(Transport transport, short opCode, byte opRespCode) {
      // 1) write header
      long messageId = writeHeader(transport, opCode);

      // 2) write key and value
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, messageId, opRespCode);
   }
}
