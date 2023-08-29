package org.infinispan.server.resp.hll;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.resp.hll.internal.CompactSet;
import org.infinispan.server.resp.hll.internal.ExplicitSet;
import org.infinispan.server.resp.hll.internal.HLLRepresentation;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.resp.hll.HyperLogLogTest")
public class HyperLogLogTest extends AbstractInfinispanTest {

   public void testUnderlyingSetUpdates() {
      HyperLogLog hll = new HyperLogLog();

      assertThat(hll.store()).isNull();

      for (int i = 0; i < 192; i++) {
         assertThat(hll.add(("hll-" + i).getBytes(StandardCharsets.US_ASCII))).isTrue();
      }

      // Explicit representation has an exact cardinality.
      assertThat(hll.store()).isInstanceOf(ExplicitSet.class);
      assertThat(hll.cardinality()).isEqualTo(192);

      // This reaches the size threshold.
      assertThat(hll.add("hll-192".getBytes(StandardCharsets.US_ASCII))).isTrue();

      // After here, we don't have an exact size anymore.
      assertThat(hll.store()).isInstanceOf(CompactSet.class);
      assertThat(hll.cardinality()).isEqualTo(193);
   }

   public void testCompactRepresentationRelativeError() {
      CompactSet hll = new CompactSet();
      long max = (long) Math.pow(10, 7);

      assertThat(hll.cardinality()).isEqualTo(0L);

      for (int i = 1; i <= max; i++) {
         hll.set(("hll-" + i).getBytes(StandardCharsets.US_ASCII));
         long current = hll.cardinality();
         // Up this point, the relative error is within the range.
         assertThat(((double) (current - i) / i)).isBetween(-0.015, 0.015);
      }
   }

   public void testSameElementDoesNotChangeCardinality() {
      CompactSet cs = new CompactSet();
      ExplicitSet es = new ExplicitSet();

      assertThat(cs.cardinality()).isEqualTo(0L);
      assertThat(es.cardinality()).isEqualTo(0L);

      byte[] value = "value".getBytes(StandardCharsets.US_ASCII);

      assertThat(cs.set(value)).isTrue();
      assertThat(es.set(value)).isTrue();

      assertThat(cs.cardinality()).isEqualTo(1L);
      assertThat(es.cardinality()).isEqualTo(1L);

      for (int i = 0; i < 1024; i++) {
         assertThat(cs.set(value)).isFalse();
         assertThat(es.set(value)).isFalse();
      }

      assertThat(cs.cardinality()).isEqualTo(1L);
      assertThat(es.cardinality()).isEqualTo(1L);
   }

   public void testConcurrentOperationsHLL() throws Exception {
      HyperLogLog hll = new HyperLogLog();
      CyclicBarrier barrier = new CyclicBarrier(3);
      Future<Void> future1 = fork(() -> {
         barrier.await();
         for (int i = 0; i < 1000; i++) {
            hll.add(("zt-" + i).getBytes(StandardCharsets.US_ASCII));
         }
      });
      Future<Void> future2 = fork(() -> {
         barrier.await();
         for (int i = 0; i < 1000; i++) {
            hll.add(("tt-" + i).getBytes(StandardCharsets.US_ASCII));
         }
      });

      barrier.await();
      future1.get(10, TimeUnit.SECONDS);
      future2.get(10, TimeUnit.SECONDS);

      assertThat(hll.store()).isInstanceOf(CompactSet.class);
      assertThat(hll.cardinality()).isEqualTo(2003L);
   }

   @Test(dataProvider = "representations")
   public void testSingleThreadOperationsRepresentation(HLLRepresentation representation, long expected) {
      assertThat(representation.cardinality()).isEqualTo(0L);
      for (int i = 0; i < 1000; i++) {
         representation.set(("zt-" + i).getBytes(StandardCharsets.US_ASCII));
      }

      for (int i = 0; i < 1000; i++) {
         representation.set(("tt-" + i).getBytes(StandardCharsets.US_ASCII));
      }

      assertThat(representation.cardinality()).isEqualTo(expected);
   }

   @Test(dataProvider = "representations")
   public void testConcurrentOperationsRepresentation(HLLRepresentation representation, long expected) throws Exception {
      assertThat(representation.cardinality()).isEqualTo(0L);
      CyclicBarrier barrier = new CyclicBarrier(3);

      Future<Void> future1 = fork(() -> {
         barrier.await();
         for (int i = 0; i < 1000; i++) {
            representation.set(("zt-" + i).getBytes(StandardCharsets.US_ASCII));
         }
      });
      Future<Void> future2 = fork(() -> {
         barrier.await();
         for (int i = 0; i < 1000; i++) {
            representation.set(("tt-" + i).getBytes(StandardCharsets.US_ASCII));
         }
      });

      barrier.await();
      future1.get(10, TimeUnit.SECONDS);
      future2.get(10, TimeUnit.SECONDS);

      assertThat(representation.cardinality()).isEqualTo(expected);
   }

   @DataProvider
   protected Object[][] representations() {
      return new Object[][] {
            {new ExplicitSet(), 2000L},
            {new CompactSet(), 2003L}
      };
   }
}
