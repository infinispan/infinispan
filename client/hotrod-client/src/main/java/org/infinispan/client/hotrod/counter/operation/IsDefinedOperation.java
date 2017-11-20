package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterManager;

/**
 * A counter operation for {@link CounterManager#isDefined(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class IsDefinedOperation extends BaseCounterOperation<Boolean> {

   public IsDefinedOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName) {
      super(codec, transportFactory, topologyId, cfg, counterName);
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_IS_DEFINED_REQUEST);
      transport.flush();

      setCacheName(header);
      short status = readHeaderAndValidate(transport, header);
      return status == NO_ERROR_STATUS;
   }
}
