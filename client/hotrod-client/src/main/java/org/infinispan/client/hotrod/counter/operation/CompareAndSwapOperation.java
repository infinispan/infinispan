package org.infinispan.client.hotrod.counter.operation;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;

/**
 * A compare-and-set operation for {@link StrongCounter#compareAndSwap(long, long)} and {@link
 * StrongCounter#compareAndSet(long, long)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CompareAndSwapOperation extends BaseCounterOperation<Long> {

   private static final Log commonsLog = LogFactory.getLog(CompareAndSwapOperation.class, Log.class);

   private final long expect;
   private final long update;
   private final CounterConfiguration counterConfiguration;

   public CompareAndSwapOperation(Codec codec, TransportFactory transportFactory, AtomicInteger topologyId,
         Configuration cfg, String counterName, long expect, long update, CounterConfiguration counterConfiguration) {
      super(codec, transportFactory, topologyId, cfg, counterName);
      this.expect = expect;
      this.update = update;
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   protected Long executeOperation(Transport transport) {
      HeaderParams header = writeHeaderAndCounterName(transport, COUNTER_CAS_REQUEST);
      transport.writeLong(expect);
      transport.writeLong(update);
      transport.flush();

      short status = readHeaderAndValidateCounter(transport, header);
      assertBoundaries(status);
      assert status == NO_ERROR_STATUS;
      return transport.readLong();
   }

   private void assertBoundaries(short status) {
      if (status == NOT_EXECUTED_WITH_PREVIOUS) {
         if (update >= counterConfiguration.upperBound()) {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.UPPER_BOUND);
         } else {
            throw commonsLog.counterOurOfBounds(CounterOutOfBoundsException.LOWER_BOUND);
         }
      }
   }
}
