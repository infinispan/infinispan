package org.infinispan.jcache;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;

import org.infinispan.commons.util.Util;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.jcache.util.InMemoryJCacheLoader;
import org.infinispan.jcache.util.InMemoryJCacheWriter;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Verifies that each JCache operation correctly applies or skips the
 * CacheLoader (read-through) and CacheWriter (write-through) based on
 * the cache configuration.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/17680">#17680</a>
 */
@Test(groups = "functional", testName = "jcache.JCacheStoreBehaviorTest")
public class JCacheStoreBehaviorTest extends AbstractInfinispanTest {

   private boolean readThrough;
   private boolean writeThrough;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new JCacheStoreBehaviorTest().config(false, false),
            new JCacheStoreBehaviorTest().config(true, false),
            new JCacheStoreBehaviorTest().config(false, true),
            new JCacheStoreBehaviorTest().config(true, true),
      };
   }

   JCacheStoreBehaviorTest config(boolean readThrough, boolean writeThrough) {
      this.readThrough = readThrough;
      this.writeThrough = writeThrough;
      return this;
   }

   @Override
   protected String parameters() {
      return "[readThrough=" + readThrough + ", writeThrough=" + writeThrough + "]";
   }

   // --- Read-through tests (CacheLoader) ---

   public void testGetLoadsWhenReadThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGet", loader, writer);
            String result = cache.get(1);

            if (readThrough) {
               assertEquals("v1", result);
               assertEquals(1, loader.getLoadCount());
            } else {
               assertNull(result);
               assertEquals(0, loader.getLoadCount());
            }
         }
      });
   }

   public void testGetAllLoadsWhenReadThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1").store(2, "v2");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAll", loader, writer);
            java.util.Map<Integer, String> result = cache.getAll(Util.asSet(1, 2));

            if (readThrough) {
               assertEquals(2, result.size());
               assertEquals("v1", result.get(1));
               assertEquals("v2", result.get(2));
               assertEquals(2, loader.getLoadCount());
            } else {
               assertTrue(result.isEmpty());
               assertEquals(0, loader.getLoadCount());
            }
         }
      });
   }

   public void testContainsKeyNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testContainsKey", loader, writer);
            boolean result = cache.containsKey(1);

            assertFalse(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testInvokeLoadsWhenReadThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testInvoke", loader, writer);
            String result = cache.invoke(1, new GetValueProcessor<>());

            if (readThrough) {
               assertEquals("v1", result);
               assertEquals(1, loader.getLoadCount());
            } else {
               assertNull(result);
               assertEquals(0, loader.getLoadCount());
            }
         }
      });
   }

   public void testInvokeAllLoadsWhenReadThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1").store(2, "v2");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testInvokeAll", loader, writer);
            java.util.Map<Integer, javax.cache.processor.EntryProcessorResult<String>> results =
                  cache.invokeAll(Util.asSet(1, 2), new GetValueProcessor<>());

            if (readThrough) {
               assertEquals(2, results.size());
               assertEquals("v1", results.get(1).get());
               assertEquals("v2", results.get(2).get());
               assertEquals(2, loader.getLoadCount());
            } else {
               assertTrue(results.isEmpty());
               assertEquals(0, loader.getLoadCount());
            }
         }
      });
   }

   // --- Write operations never load from CacheLoader ---

   public void testPutNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPut", loader, writer);
            cache.put(1, "new");

            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testGetAndPutNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndPut", loader, writer);
            String prev = cache.getAndPut(1, "new");

            assertNull(prev);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testPutAllNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1").store(2, "v2");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPutAll", loader, writer);
            java.util.Map<Integer, String> map = new java.util.HashMap<>();
            map.put(1, "new1");
            map.put(2, "new2");
            cache.putAll(map);

            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testPutIfAbsentNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPutIfAbsent", loader, writer);
            boolean result = cache.putIfAbsent(1, "new");

            assertTrue(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testRemoveNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testRemove", loader, writer);
            boolean result = cache.remove(1);

            assertFalse(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testRemoveConditionalNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testRemoveCond", loader, writer);
            boolean result = cache.remove(1, "v1");

            assertFalse(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testGetAndRemoveNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndRemove", loader, writer);
            String result = cache.getAndRemove(1);

            assertNull(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testReplaceNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testReplace", loader, writer);
            boolean result = cache.replace(1, "new");

            assertFalse(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testReplaceConditionalNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testReplaceCond", loader, writer);
            boolean result = cache.replace(1, "v1", "new");

            assertFalse(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testGetAndReplaceNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndReplace", loader, writer);
            String result = cache.getAndReplace(1, "new");

            assertNull(result);
            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   public void testRemoveAllNeverLoads() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            loader.store(1, "v1");
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testRemoveAll", loader, writer);
            cache.removeAll(Util.asSet(1));

            assertEquals(0, loader.getLoadCount());
         }
      });
   }

   // --- Write-through tests (CacheWriter) ---

   public void testPutWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPutWrite", loader, writer);
            cache.put(1, "v1");

            if (writeThrough) {
               assertEquals(1, writer.getWriteCount());
               assertEquals("v1", writer.get(1));
            } else {
               assertEquals(0, writer.getWriteCount());
            }
         }
      });
   }

   public void testGetAndPutWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndPutWrite", loader, writer);
            cache.getAndPut(1, "v1");

            if (writeThrough) {
               assertEquals(1, writer.getWriteCount());
               assertEquals("v1", writer.get(1));
            } else {
               assertEquals(0, writer.getWriteCount());
            }
         }
      });
   }

   public void testPutAllWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPutAllWrite", loader, writer);
            java.util.Map<Integer, String> map = new java.util.HashMap<>();
            map.put(1, "v1");
            map.put(2, "v2");
            cache.putAll(map);

            if (writeThrough) {
               assertEquals(2, writer.getWriteCount());
               assertEquals("v1", writer.get(1));
               assertEquals("v2", writer.get(2));
            } else {
               assertEquals(0, writer.getWriteCount());
            }
         }
      });
   }

   public void testPutIfAbsentWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testPutIfAbsentWrite", loader, writer);
            cache.putIfAbsent(1, "v1");

            if (writeThrough) {
               assertEquals(1, writer.getWriteCount());
            } else {
               assertEquals(0, writer.getWriteCount());
            }
         }
      });
   }

   public void testRemoveDeletesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testRemoveWrite", loader, writer);
            cache.put(1, "v1");
            long writesBefore = writer.getWriteCount();
            cache.remove(1);

            if (writeThrough) {
               assertEquals(1, writer.getDeleteCount());
            } else {
               assertEquals(0, writer.getDeleteCount());
            }
         }
      });
   }

   public void testGetAndRemoveDeletesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndRemoveWrite", loader, writer);
            cache.put(1, "v1");
            cache.getAndRemove(1);

            if (writeThrough) {
               assertEquals(1, writer.getDeleteCount());
            } else {
               assertEquals(0, writer.getDeleteCount());
            }
         }
      });
   }

   public void testReplaceWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testReplaceWrite", loader, writer);
            cache.put(1, "v1");
            long writesBefore = writer.getWriteCount();
            cache.replace(1, "v2");

            if (writeThrough) {
               assertTrue(writer.getWriteCount() > writesBefore);
               assertEquals("v2", writer.get(1));
            } else {
               assertEquals(writesBefore, writer.getWriteCount());
            }
         }
      });
   }

   public void testGetAndReplaceWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testGetAndReplaceWrite", loader, writer);
            cache.put(1, "v1");
            long writesBefore = writer.getWriteCount();
            cache.getAndReplace(1, "v2");

            if (writeThrough) {
               assertTrue(writer.getWriteCount() > writesBefore);
               assertEquals("v2", writer.get(1));
            } else {
               assertEquals(writesBefore, writer.getWriteCount());
            }
         }
      });
   }

   public void testRemoveAllSetDeletesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testRemoveAllSetWrite", loader, writer);
            cache.put(1, "v1");
            cache.put(2, "v2");
            cache.removeAll(Util.asSet(1, 2));

            if (writeThrough) {
               assertEquals(2, writer.getDeleteCount());
            } else {
               assertEquals(0, writer.getDeleteCount());
            }
         }
      });
   }

   public void testClearNeverWritesToStore() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testClearWrite", loader, writer);
            cache.put(1, "v1");
            cache.put(2, "v2");
            long writesBefore = writer.getWriteCount();
            long deletesBefore = writer.getDeleteCount();
            cache.clear();

            assertEquals(writesBefore, writer.getWriteCount());
            assertEquals(deletesBefore, writer.getDeleteCount());
         }
      });
   }

   // --- invoke write-through ---

   public void testInvokeSetValueWritesWhenWriteThrough() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(true)) {
         @Override
         public void call() {
            InMemoryJCacheLoader<Integer, String> loader = new InMemoryJCacheLoader<>();
            InMemoryJCacheWriter<Integer, String> writer = new InMemoryJCacheWriter<>();

            Cache<Integer, String> cache = createCache(cm, "testInvokeWrite", loader, writer);
            cache.invoke(1, new SetValueProcessor<>(), "v1");

            if (writeThrough) {
               assertEquals(1, writer.getWriteCount());
               assertEquals("v1", writer.get(1));
            } else {
               assertEquals(0, writer.getWriteCount());
            }
         }
      });
   }

   // --- Helper ---

   @SuppressWarnings("unchecked")
   private Cache<Integer, String> createCache(
         org.infinispan.manager.EmbeddedCacheManager cm,
         String name,
         InMemoryJCacheLoader<Integer, String> loader,
         InMemoryJCacheWriter<Integer, String> writer) {

      MutableConfiguration<Integer, String> cfg = new MutableConfiguration<>();
      cfg.setStoreByValue(false);
      cfg.setReadThrough(readThrough);
      cfg.setWriteThrough(writeThrough);
      cfg.setCacheLoaderFactory(FactoryBuilder.factoryOf(loader));
      cfg.setCacheWriterFactory(FactoryBuilder.factoryOf(writer));

      JCacheManager jCacheManager = new JCacheManager(
            URI.create(getClass().getName() + "." + readThrough + "." + writeThrough), cm, null);
      return jCacheManager.createCache(name, cfg);
   }

   private static class GetValueProcessor<K, V> implements EntryProcessor<K, V, V>, java.io.Serializable {
      @Override
      public V process(MutableEntry<K, V> entry, Object... arguments) {
         return entry.getValue();
      }
   }

   private static class SetValueProcessor<K, V> implements EntryProcessor<K, V, Void>, java.io.Serializable {
      @Override
      @SuppressWarnings("unchecked")
      public Void process(MutableEntry<K, V> entry, Object... arguments) {
         entry.setValue((V) arguments[0]);
         return null;
      }
   }
}
