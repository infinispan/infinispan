package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A counter operation for {@link CounterManager#remove(String)}, {@link StrongCounter#remove()} and {@link
 * WeakCounter#remove()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveOperation extends BaseCounterOperation<Void> {
   public RemoveOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName) {
      super(codec, transportFactory, topologyId, cfg, counterName);
   }

   @Override
   protected Void executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_REMOVE_REQUEST);
      transport.flush();
      readHeaderAndValidateCounter(transport, header);
      return null;
   }
}
