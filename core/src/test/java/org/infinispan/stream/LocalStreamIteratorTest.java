package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test to verify stream behavior for a local cache.
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.LocalStreamIteratorTest")
public class LocalStreamIteratorTest extends BaseStreamIteratorTest {
   public LocalStreamIteratorTest() {
      super(false, CacheMode.LOCAL);
   }

   protected final AtomicInteger counter = new AtomicInteger();

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return cache.toString() + "-" + counter.getAndIncrement();
   }
}
