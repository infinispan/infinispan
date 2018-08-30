package org.infinispan.manager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tests for a single cluster executor
 * @author Will Burns
 * @since 9.1
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.SingleClusterExecutorTest")
public class SingleClusterExecutorTest extends AllClusterExecutorTest {
   static final AtomicInteger atomicInteger = new AtomicInteger();

   private boolean local;

   public SingleClusterExecutorTest() {
      atomicIntegerSupplier = () -> atomicInteger;
   }

   SingleClusterExecutorTest executeLocal(boolean local) {
      this.local = local;
      return this;
   }

   @Override
   protected String parameters() {
      return "[" + local + "]";
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new SingleClusterExecutorTest().executeLocal(true),
            new SingleClusterExecutorTest().executeLocal(false)
      };
   }

   ClusterExecutor executor(EmbeddedCacheManager cm) {
      return cm.executor().singleNodeSubmission().filterTargets(a -> local == a.equals(cm.getAddress()));
   }

   void assertSize(EmbeddedCacheManager[] cms, int receivedSize) {
      assertEquals(1, receivedSize);
   }

   void eventuallyAssertSize(EmbeddedCacheManager[] cms, Supplier<Integer> supplier) {
      eventuallyEquals(1, supplier);
   }

   void assertContains(EmbeddedCacheManager[] managers, Collection<Address> results) {
      boolean contains = false;
      for (EmbeddedCacheManager manager : managers) {
         if (results.contains(manager.getAddress())) {
            contains = true;
            break;
         }
      }
      assertTrue("At least 1 manager from " + Arrays.toString(managers) + " should be in " + results, contains);
   }
}
