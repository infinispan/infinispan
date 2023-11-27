package org.infinispan.jcache;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Add {@link Cache#invoke(Object, javax.cache.processor.EntryProcessor, Object...)} tests covering edge cases missing
 * in the TCK.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "jcache.InvokeProcessorTest")
public class InvokeProcessorTest extends AbstractInfinispanTest {

   public void testInvokeProcesorStoreByValueException(Method m) {
      invokeProcessorThrowsException(m,
            new MutableConfiguration<>(),
            new ArrayList<>(Arrays.asList(1, 2, 3)));
   }

   public void testInvokeProcesorStoreByReferenceException(Method m) {
      // As per: https://github.com/jsr107/jsr107spec/issues/106
      invokeProcessorThrowsException(m,
            new MutableConfiguration<String, Entry>().setStoreByValue(false),
            new ArrayList<>(Arrays.asList(1, 2, 3, 4)));
   }

   public void testInvokeProcesorStoreByValue(Method m) {
      invokeProcessor(m, new MutableConfiguration<>());
   }

   private void invokeProcessorThrowsException(Method m, final MutableConfiguration<String, Entry> jcacheCfg,
                                               final List<Integer> expectedValue) {
      final String name = getName(m);
      CacheManager cm = new JCacheManager(URI.create(name), TestCacheManagerFactory.createCacheManager(JCacheTestSCI.INSTANCE), null);
      Cache<String, Entry> cache = cm.createCache(name, jcacheCfg);
      final String query = "select * from x";
      cache.put(query, new Entry(new ArrayList<>(Arrays.asList(1, 2, 3))));
      try {
         cache.invoke(query, new TestExceptionThrowingEntryProcessor());
         fail("Expected an exception to be thrown");
      } catch (CacheException e) {
         assertTrue(e.getCause() instanceof UnexpectedException);
      }
      assertEquals(expectedValue, cache.get(query).integers);
   }

   private void invokeProcessor(Method m, final MutableConfiguration<String, Entry> jcacheCfg) {
      final String name = getName(m);
      CacheManager cm = new JCacheManager(URI.create(name), TestCacheManagerFactory.createCacheManager(JCacheTestSCI.INSTANCE), null);
      Cache<String, Entry> cache = cm.createCache(name, jcacheCfg);
      ArrayList<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3));
      final String query = "select * from x";
      cache.put(query, new Entry(list));
      cache.invoke(query, new TestEntryProcessor());
      assertEquals(new ArrayList<>(Arrays.asList(1, 2, 3, 4)), cache.get(query).integers);
   }

   private String getName(Method m) {
      return getClass().getName() + '.' + m.getName();
   }

   private static class UnexpectedException extends RuntimeException {
   }

   public static class Entry {

      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      List<Integer> integers;

      @ProtoFactory
      Entry(ArrayList<Integer> integers) {
         this.integers = integers;
      }
   }

   @ProtoName("TestEntryProcessor")
   public static class TestEntryProcessor implements EntryProcessor<String, Entry, Object> {
      @Override
      public Object process(MutableEntry<String, Entry> entry, Object... objects) throws EntryProcessorException {
         Entry e = entry.getValue();
         e.integers.add(4);
         entry.setValue(e);
         return null;
      }
   }

   @ProtoName("TestExceptionThrowingEntryProcessor")
   public static class TestExceptionThrowingEntryProcessor implements EntryProcessor<String, Entry, Object> {
      @Override
      public Object process(MutableEntry<String, Entry> entry, Object... objects) throws EntryProcessorException {
         entry.getValue().integers.add(4);
         throw new UnexpectedException();
      }
   }
}
