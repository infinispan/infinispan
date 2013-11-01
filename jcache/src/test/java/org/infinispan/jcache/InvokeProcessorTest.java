package org.infinispan.jcache;

import org.infinispan.jcache.util.JCacheRunnable;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.infinispan.jcache.util.JCacheTestingUtil.withCachingProvider;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Add {@link Cache#invoke(Object, javax.cache.processor.EntryProcessor, Object...)}
 * tests covering edge cases missing in the TCK.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "jcache.InvokeProcessorTest")
public class InvokeProcessorTest {

   public void testInvokeProcesorStoreByValueException(Method m) {
      invokeProcessorThrowsException(m,
            new MutableConfiguration<String, List<Integer>>(),
            new ArrayList<Integer>(Arrays.asList(1, 2, 3)));
   }

   public void testInvokeProcesorStoreByReferenceException(Method m) {
      // As per: https://github.com/jsr107/jsr107spec/issues/106
      invokeProcessorThrowsException(m,
            new MutableConfiguration<String, List<Integer>>().setStoreByValue(false),
            new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4)));
   }

   public void testInvokeProcesorStoreByValue(Method m) {
      invokeProcessor(m, new MutableConfiguration<String, List<Integer>>());
   }

   private void invokeProcessorThrowsException(
         Method m, final MutableConfiguration<String, List<Integer>> jcacheCfg,
         final List<Integer> expectedValue) {
      final String name = getName(m);
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            CacheManager cm = provider.getCacheManager();
            Cache<String, List<Integer>> cache = cm.createCache(name, jcacheCfg);
            List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
            final String query = "select * from x";
            cache.put(query, list);
            try {
               cache.invoke(query,
                     new EntryProcessor<String, List<Integer>, Object>() {
                        @Override
                        public Object process(MutableEntry<String, List<Integer>> entry, Object... arguments) {
                           entry.getValue().add(4);
                           throw new UnexpectedException();
                        }
                     });
               fail("Expected an exception to be thrown");
            } catch (CacheException e) {
               assertTrue(e.getCause() instanceof UnexpectedException);
            }

            assertEquals(expectedValue, cache.get(query));
         }
      });
   }

   private void invokeProcessor(
         Method m, final MutableConfiguration<String, List<Integer>> jcacheCfg) {
      final String name = getName(m);
      withCachingProvider(new JCacheRunnable() {
         @Override
         public void run(CachingProvider provider) {
            CacheManager cm = provider.getCacheManager();
            Cache<String, List<Integer>> cache = cm.createCache(name, jcacheCfg);
            List<Integer> list = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
            final String query = "select * from x";
            cache.put(query, list);
            cache.invoke(query,
                  new EntryProcessor<String, List<Integer>, Object>() {
                     @Override
                     public Object process(MutableEntry<String, List<Integer>> entry, Object... arguments) {
                        List<Integer> ids = entry.getValue();
                        ids.add(4);
                        entry.setValue(ids);
                        return null;
                     }
                  });

            assertEquals(new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4)),
                  cache.get(query));
         }
      });
   }

   private String getName(Method m) {
      return getClass().getName() + '.' + m.getName();
   }

   private static class UnexpectedException extends RuntimeException {}

}
