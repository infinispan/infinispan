package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.exception.CounterOutOfBoundsException;

/**
 * Add operation.
 * <p>
 * Adds the {@code delta} to the counter's value and returns the result.
 * <p>
 * It can throw a {@link CounterOutOfBoundsException} if the counter is bounded and the it has been reached.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class AddOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(AddOperation.class, Log.class);

   private final long delta;

   public AddOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId, Configuration cfg,
         String counterName, long delta) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.delta = delta;
   }

   @Override
   protected Long executeOperation(Transport transport) {
      HeaderParams params = writeHeaderAndCounterName(transport, COUNTER_ADD_AND_GET_REQUEST);
      transport.writeLong(delta);
      transport.flush();

      short status = readHeaderAndValidateCounter(transport, params);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      return transport.readLong();
   }

   private void assertBoundaries(short status) {
      if (status == NOT_EXECUTED_WITH_PREVIOUS) {
         if (delta > 0) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
