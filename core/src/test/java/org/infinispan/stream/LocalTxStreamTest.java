package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a local stream with transactions
 */
@Test(groups = "functional", testName = "streams.LocalTxStreamTest")
public class LocalTxStreamTest extends BaseStreamTest {
   public LocalTxStreamTest() {
      super(true);
      cacheMode(CacheMode.LOCAL);
   }

   @Override
   protected <E> CacheStream<E> createStream(CacheCollection<E> entries) {
      return entries.stream();
   }

   @Test(enabled = false)
   @Override
   public void testKeySegmentFilter() {

   }

   private void testInTransactionWithValues(Map<Integer, String> values, Consumer<CacheSet<Map.Entry<Integer, String>>> setConsumer)
         throws SystemException, NotSupportedException {
      Cache<Integer, String> cache = getCache(0);

      tm(cache).begin();
      try {
         cache.putAll(values);

         setConsumer.accept(cache.entrySet());
      } finally {
         tm(cache).rollback();
      }
   }

   public void testTransactionalFindFirstPresent() throws SystemException, NotSupportedException {
      testInTransactionWithValues(Map.of(1, "foo"), set -> assertTrue(set.stream().findFirst().isPresent()));
   }

   public void testTransactionalFindFirstFiltered() throws SystemException, NotSupportedException {
      testInTransactionWithValues(Map.of(1, "foo"), set -> assertTrue(set.stream()
            .filter(e -> e.getValue().equals("bar")).findFirst().isEmpty()));
   }

   public void testTransactionalFindFirstInCacheAndTx() throws SystemException, NotSupportedException {
      Cache<Integer, String> cache = getCache(0);
      cache.put(1, "original");
      testInTransactionWithValues(Map.of(1, "foo"), set -> {
         Optional<Map.Entry<Integer, String>> op = set.stream().findFirst();
         assertTrue(op.isPresent());
         assertEquals("foo", op.get().getValue());
      });
   }
}
