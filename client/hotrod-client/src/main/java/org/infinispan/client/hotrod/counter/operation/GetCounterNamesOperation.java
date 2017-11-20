package org.infinispan.client.hotrod.counter.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.counter.api.CounterManager;

/**
 * A counter operation for {@link CounterManager#getCounterNames()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetCounterNamesOperation extends BaseCounterOperation<Collection<String>> {

   public GetCounterNamesOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg) {
      super(codec, transportFactory, topologyId, cfg, "");
   }

   @Override
   protected Collection<String> executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, COUNTER_GET_NAMES_REQUEST);
      transport.flush();

      setCacheName(params);
      short status = readHeaderAndValidate(transport, params);
      assert status == NO_ERROR_STATUS;
      int size = transport.readVInt();
      Collection<String> names = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         names.add(transport.readString());
      }
      return names;
   }
}
