package org.infinispan.client.hotrod.impl.iteration;

import static org.testng.Assert.assertEquals;

import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.SegmentFilteredFailOverTest")
public class SegmentFilteredFailOverTest extends DistFailOverRemoteIteratorTest {

   static final int ENTRIES = 100;

   @Override
   public void testFailOver() throws InterruptedException {
      RemoteCache<Integer, AccountHS> remoteCache = clients.get(0).getCache();
      populateCache(ENTRIES, this::newAccount, remoteCache);

      Cache<Object, Object> cache = caches().get(0);
      int totalSegments = cache.getCacheConfiguration().clustering().hash().numSegments();

      Set<Integer> segments = IntStream.rangeClosed(0, totalSegments / 2).boxed().collect(Collectors.toSet());

      long expectedCount = cache.keySet().stream().filterKeySegments(segments).count();

      int actualCount = 0;
      try (CloseableIterator<Entry<Object, Object>> iterator = remoteCache.retrieveEntries(null, segments, 100)) {
         for (int i = 0; i < ENTRIES / 5; i++) {
            iterator.next();
            actualCount++;
         }

         killIterationServer();

         while (iterator.hasNext()) {
            iterator.next();
            actualCount++;
         }
      }

      assertEquals(actualCount, expectedCount);
   }
}
