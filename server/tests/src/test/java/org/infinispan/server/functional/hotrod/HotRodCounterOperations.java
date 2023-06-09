package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCounterOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testCounters() {
      CounterManager counterManager = SERVERS.getCounterManager();

      counterManager.defineCounter("c1", CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
            .upperBound(10)
            .initialValue(1)
            .build());

      counterManager.defineCounter("c2", CounterConfiguration.builder(CounterType.WEAK)
            .initialValue(5)
            .build());

      SyncStrongCounter c1 = counterManager.getStrongCounter("c1").sync();
      SyncWeakCounter c2 = counterManager.getWeakCounter("c2").sync();

      assertEquals(1, c1.getValue());
      assertEquals(5, c2.getValue());
   }
}
