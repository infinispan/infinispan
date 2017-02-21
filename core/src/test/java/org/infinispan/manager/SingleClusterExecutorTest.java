package org.infinispan.manager;

import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests for a single cluster executor
 * @author Will Burns
 * @since 9.1
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.SingleClusterExecutorTest")
public class SingleClusterExecutorTest extends AllClusterExecutorTest {
   static final AtomicInteger atomicInteger = new AtomicInteger();

   public SingleClusterExecutorTest() {
      atomicIntegerSupplier = () -> atomicInteger;
   }

   ClusterExecutor executor(EmbeddedCacheManager cm) {
      return cm.executor().singleNodeSubmission();
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
