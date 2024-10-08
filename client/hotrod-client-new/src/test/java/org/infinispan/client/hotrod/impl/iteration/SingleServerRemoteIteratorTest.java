package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.impl.iteration.Util.assertForAll;
import static org.infinispan.client.hotrod.impl.iteration.Util.extractKeys;
import static org.infinispan.client.hotrod.impl.iteration.Util.extractValues;
import static org.infinispan.client.hotrod.impl.iteration.Util.populateCache;
import static org.infinispan.client.hotrod.impl.iteration.Util.rangeAsSet;
import static org.infinispan.client.hotrod.impl.iteration.Util.setOf;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.dsl.embedded.DslSCI;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.SingleServerRemoteIteratorTest")
public class SingleServerRemoteIteratorTest extends SingleHotRodServerTest {

   public static final String FILTER_CONVERTER_FACTORY_NAME = "even-accounts-descriptions";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(DslSCI.INSTANCE, hotRodCacheConfiguration());
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort()).addContextInitializer(DslSCI.INSTANCE);
      return new RemoteCacheManager(builder.build());
   }

   @Test
   public void testEmptyCache() {
      try (CloseableIterator<Entry<Object, Object>> iterator = remoteCacheManager.getCache().retrieveEntries(null, null, 100)) {
         assertFalse(iterator.hasNext());
      }
   }

   @Test
   public void testEmptySegments() {
      populateCache(1, i -> "value " + i, remoteCacheManager.getCache());
      try (CloseableIterator<Entry<Object, Object>> iterator = remoteCacheManager.getCache().retrieveEntries(null, Collections.emptySet(), 100)) {
         assertFalse(iterator.hasNext());
      }
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN005060.*")
   public void testEmptyFilter() {
      try (CloseableIterator<Entry<Object, Object>> iterator = remoteCacheManager.getCache().retrieveEntries("", null, 100)) {
         assertFalse(iterator.hasNext());
      }
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testException1() {
      CloseableIterator<Entry<Object, Object>> iterator = null;
      try {
         iterator = remoteCacheManager.getCache().retrieveEntries(null, null, 100);
         iterator.next();
      } finally {
         if (iterator != null) {
            iterator.close();
         }
      }
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testException2() {
      populateCache(3, i -> "value " + i, remoteCacheManager.getCache());
      CloseableIterator<Entry<Object, Object>> iterator = remoteCacheManager.getCache().retrieveEntries(null, null, 100);
      iterator.close();
      iterator.next();
   }

   public void testResultsSingleFetch() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();

      populateCache(3, i -> "value " + i, cache);

      Set<Entry<Object, Object>> entries = new HashSet<>();

      try (CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, 5)) {
         entries.add(iterator.next());
         entries.add(iterator.next());
         entries.add(iterator.next());
      }

      Set<Integer> keys = extractKeys(entries);
      Set<String> values = extractValues(entries);

      assertEquals(keys, setOf(0, 1, 2));
      assertEquals(values, setOf("value 0", "value 1", "value 2"));
   }

   public void testResultsMultipleFetches() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();

      int cacheSize = 100;
      populateCache(cacheSize, i -> "value " + i, cache);

      Set<Entry<Object, Object>> entries = new HashSet<>();

      try (CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, 5)) {
         while (iterator.hasNext()) {
            entries.add(iterator.next());
         }
      }

      Set<Integer> keys = extractKeys(entries);
      assertEquals(rangeAsSet(0, cacheSize), keys);
   }

   public void testEntities() {
      RemoteCache<Integer, AccountHS> cache = remoteCacheManager.getCache();

      int cacheSize = 50;
      populateCache(cacheSize, Util::newAccount, cache);

      Set<Entry<Object, Object>> entries = new HashSet<>();

      try (CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(null, null, 5)) {
         while (iterator.hasNext()) {
            entries.add(iterator.next());
         }
      }

      assertEquals(cacheSize, entries.size());

      Set<AccountHS> values = extractValues(entries);

      assertForAll(values, v -> v != null);
      assertForAll(values, v -> v.getId() < cacheSize);
   }

   public void testFilterConverter() {
      hotrodServer.addKeyValueFilterConverterFactory(FILTER_CONVERTER_FACTORY_NAME, () ->
            new AbstractKeyValueFilterConverter<Integer, AccountHS, String>() {
               @Override
               public String filterAndConvert(Integer key, AccountHS value, Metadata metadata) {
                  if (!(key % 2 == 0)) {
                     return null;
                  }
                  return value.getDescription();
               }
            });

      RemoteCache<Integer, AccountHS> cache = remoteCacheManager.getCache();

      int cacheSize = 50;
      populateCache(cacheSize, Util::newAccount, cache);

      Set<Entry<Object, Object>> entries = new HashSet<>();

      try (CloseableIterator<Entry<Object, Object>> iterator = cache.retrieveEntries(FILTER_CONVERTER_FACTORY_NAME, null, 5)) {
         while (iterator.hasNext()) {
            entries.add(iterator.next());
         }
      }

      assertEquals(cacheSize / 2, entries.size());

      Set<Integer> keys = extractKeys(entries);
      assertForAll(keys, v -> v % 2 == 0);
   }
}
