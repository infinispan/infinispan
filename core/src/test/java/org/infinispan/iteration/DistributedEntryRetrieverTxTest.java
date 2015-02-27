package org.infinispan.iteration;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test to verify distributed entry behavior when in a tx
 *
 * @author wburns
 * @since 7.2
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedEntryRetrieverTxTest")
public class DistributedEntryRetrieverTxTest extends DistributedEntryRetrieverTest {
   public DistributedEntryRetrieverTxTest() {
      super(true);
   }

   public void testFilterWithExistingTransaction() throws NotSupportedException,
         SystemException, SecurityException, IllegalStateException, RollbackException,
         HeuristicMixedException, HeuristicRollbackException {
      Map<Object, String> values = putValueInEachCache(3);

      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = TestingUtil.extractComponent(cache,
            TransactionManager.class);
      tm.begin();
      try {
         Object key = "filtered-key";
         cache.put(key, "filtered-value");

         CloseableIterator<CacheEntry<Object, String>> iterator = 
               cache.getAdvancedCache().filterEntries(
                     new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(Collections.singleton(key)))).iterator();
         Map<Object, String> results = mapFromIterator(iterator);
         assertEquals(values, results);
      } finally {
         tm.rollback();
      }
   }

   @Test
   public void testConverterWithExistingTransaction() throws NotSupportedException, SystemException {
      Map<Object, String> values = putValuesInCache();

      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = TestingUtil.extractComponent(cache,
            TransactionManager.class);
      tm.begin();
      try {
         Object key = "converted-key";
         String value = "converted-value";
         values.put(key, value);
         cache.put(key, "converted-value");
         
         try (EntryIterable<Object, String> iterable = cache.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance())) {
            Map<Object, String> results = mapFromIterable(iterable.converter(new StringTruncator(2, 5)));

            assertEquals(values.size(), results.size());
            for (Map.Entry<Object, String> entry : values.entrySet()) {
               assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
            }
         }
      } finally {
         tm.rollback();
      }
   }

   @Test
   public void testKeyFilterConverterWithExistingTransaction() throws NotSupportedException, SystemException {
      Map<Object, String> values = putValuesInCache();


      Cache<Object, String> cache = cache(0, CACHE_NAME);
      TransactionManager tm = TestingUtil.extractComponent(cache,
            TransactionManager.class);
      tm.begin();
      try {
         Iterator<Map.Entry<Object, String>> iter = values.entrySet().iterator();
         Map.Entry<Object, String> extraEntry = iter.next();
         while (iter.hasNext()) {
            iter.next();
            iter.remove();
         }
         
         Object key = "converted-key";
         String value = "converted-value";
         values.put(key, value);
         cache.put(key, "converted-value");

         Collection<Object> acceptedKeys = new ArrayList<>();
         acceptedKeys.add(key);
         acceptedKeys.add(extraEntry.getKey());
         
         KeyValueFilterConverter<Object, String, String> filterConverter = 
               new CompositeKeyValueFilterConverter<Object, String, String>(
                     new KeyFilterAsKeyValueFilter<>(new CollectionKeyFilter<>(acceptedKeys, true)),
                     new StringTruncator(2, 5));
         try (EntryIterable<Object, String> iterable = cache.getAdvancedCache().filterEntries(filterConverter)) {
            Map<Object, String> results = mapFromIterable(iterable.converter(filterConverter));
            assertEquals(values.size(), results.size());
            for (Map.Entry<Object, String> entry : values.entrySet()) {
               assertEquals(entry.getValue().substring(2, 7), results.get(entry.getKey()));
            }
         }
      } finally {
         tm.rollback();
      }
   }
}
