package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;

/**
 * A counter define operation for {@link CounterManager#defineCounter(String, CounterConfiguration)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class DefineCounterOperation extends BaseCounterOperation<Boolean> {

   private final CounterConfiguration configuration;

   public DefineCounterOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName, CounterConfiguration configuration) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.configuration = configuration;
   }

   @Override
   protected Boolean executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_CREATE_REQUEST);
      encodeConfiguration(configuration, transport::writeByte, transport::writeLong, transport::writeVInt);
      transport.flush();

      return readHeaderAndValidateCounter(transport, header) == NO_ERROR_STATUS;
   }
}
