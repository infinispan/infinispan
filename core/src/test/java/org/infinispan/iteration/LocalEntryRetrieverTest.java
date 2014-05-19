package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test to verify entry retriever behavior for a local cache.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.LocalEntryRetrieverTest")
public class LocalEntryRetrieverTest extends BaseEntryRetrieverTest {
   public LocalEntryRetrieverTest() {
      super(false, CacheMode.LOCAL);
   }

   protected final AtomicInteger counter = new AtomicInteger();

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return cache.toString() + "-" + counter.getAndIncrement();
   }
}
