package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * A counter operation that returns the counter's value.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetValueOperation extends BaseCounterOperation<Long> {

   public GetValueOperation(Codec codec,
         TransportFactory transportFactory,
         AtomicInteger topologyId,
         Configuration cfg, String counterName) {
      super(codec, transportFactory, topologyId, cfg, counterName);
   }

   @Override
   protected Long executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_GET_REQUEST);
      transport.flush();
      short status = readHeaderAndValidateCounter(transport, header);
      assert status == NO_ERROR_STATUS;
      return transport.readLong();
   }
}
