package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;

/**
 * Test to verify entry retriever behavior for a local cache
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
