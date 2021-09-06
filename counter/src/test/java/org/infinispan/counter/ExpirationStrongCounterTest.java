package org.infinispan.counter;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.counter.exception.CounterOutOfBoundsException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "counter.ExpirationStrongCounterTest")
public class ExpirationStrongCounterTest extends MultipleCacheManagersTest {

   private static final long EXPIRATION_MILLISECONDS = 1000;
   private final ControlledTimeService timeService = new ControlledTimeService("expiring-strong-counter");

   public void testUnboundedCounter() {
      CounterConfiguration config = CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG)
            .lifespan(EXPIRATION_MILLISECONDS).build();
      String counterName = "unbounded-counter";
      CounterManager counterManager = counterManager();
      counterManager.defineCounter(counterName, config);
      doTest(counterName, -1);
   }

   public void testBoundedCounter() {
      CounterConfiguration config = CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
            .lifespan(EXPIRATION_MILLISECONDS)
            .upperBound(3).build();
      String counterName = "bounded-counter";
      CounterManager counterManager = counterManager();
      counterManager.defineCounter(counterName, config);
      doTest(counterName, 3);
   }

   public void testBoundedCounterNeverReached() {
      CounterConfiguration config = CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
            .lifespan(EXPIRATION_MILLISECONDS)
            .upperBound(30).build();
      String counterName = "bounded-counter-2";
      CounterManager counterManager = counterManager();
      counterManager.defineCounter(counterName, config);
      doTest(counterName, 30);
   }

   private void doTest(String counterName, long upperBound) {
      SyncStrongCounter counter = counterManager().getStrongCounter(counterName).sync();
      if (upperBound == -1) {
         incrementUnbound(counter, 0);
      } else {
         incrementBound(counter, 0, upperBound);
      }

      // expire the counter
      timeService.advance(EXPIRATION_MILLISECONDS + 1);

      // after the time expired, the counter should be in its initial value
      assertEquals(0, counter.getValue());

      if (upperBound == -1) {
         incrementUnbound(counter, 0);
      } else {
         incrementBound(counter, 0, upperBound);
      }

      // this should not expire the counter
      timeService.advance(EXPIRATION_MILLISECONDS - 1);

      if (upperBound == -1) {
         assertEquals(5, counter.getValue());
         incrementUnbound(counter, 5);
      } else {
         assertEquals(Math.min(5, upperBound), counter.getValue());
         incrementBound(counter, Math.min(5, upperBound), upperBound);
      }

      // expire the counter
      timeService.advance(2);

      // after the time expired, the counter should be in its initial value
      assertEquals(0, counter.getValue());

   }

   private void incrementUnbound(SyncStrongCounter counter, long expectedBaseValue) {
      for (int i = 0; i < 5; ++i) {
         incrementAndAssert(counter, expectedBaseValue + i + 1);
      }
      assertEquals(expectedBaseValue + 5, counter.getValue());
   }

   private void incrementBound(SyncStrongCounter counter, long initialValue, long upperBound) {
      for (int i = 0; i < 5; ++i) {
         if (counter.getValue() == upperBound) {
            expectException(CounterOutOfBoundsException.class, counter::incrementAndGet);
         } else {
            incrementAndAssert(counter, initialValue + i + 1);
         }
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(2);
      cacheManagers.forEach(m -> replaceComponent(m, TimeService.class, timeService, true));
   }

   private CounterManager counterManager() {
      return EmbeddedCounterManagerFactory.asCounterManager(manager(0));
   }

   private void incrementAndAssert(SyncStrongCounter counter, long expectedValue) {
      assertEquals(expectedValue, counter.incrementAndGet());
   }
}
