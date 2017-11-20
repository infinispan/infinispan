package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.util.EncodeUtil.decodeConfiguration;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;

/**
 * A counter configuration for {@link CounterManager#getConfiguration(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetConfigurationOperation extends BaseCounterOperation<CounterConfiguration> {

   public GetConfigurationOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName) {
      super(codec, transportFactory, topologyId, cfg, counterName);
   }

   @Override
   protected CounterConfiguration executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_GET_CONFIGURATION_REQUEST);
      transport.flush();

      //don't use readHeaderAndValidateCounter since we don't want an exception if the counter doesn't exist.
      setCacheName(header);
      int status = readHeaderAndValidate(transport, header);
      if (status != NO_ERROR_STATUS) {
         return null;
      }

      return decodeConfiguration(() -> (byte) transport.readByte(), transport::readLong, transport::readVInt);
   }
}
