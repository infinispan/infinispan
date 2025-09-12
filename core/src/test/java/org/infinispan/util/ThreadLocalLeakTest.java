package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import java.io.File;
import java.lang.ref.Reference;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests whether certain cache set ups result in thread local leaks.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.ThreadLocalLeakTest")
public class ThreadLocalLeakTest extends AbstractInfinispanTest {

   private static final Pattern THREAD_LOCAL_FILTER = Pattern.compile("org\\.infinispan\\..*");

   // Ued to ignore the thread-local in our ConcurrentHashMap backport
   private static final Set<String> ACCEPTED_THREAD_LOCALS = new HashSet<>(Arrays.asList());

   private final ThreadLocal<ThreadLocalLeakTest> DUMMY_THREAD_LOCAL = ThreadLocal.withInitial(() -> this);

   private String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      org.infinispan.commons.util.Util.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   public void testCheckThreadLocalLeaks() throws Exception {
      // Perform the test in a new thread so we don't have any thread-locals from previous tests
      fork(this::doCheckThreadLocalLeaks).get(30, TimeUnit.SECONDS);
   }

   private void doCheckThreadLocalLeaks() throws Exception {
      TestResourceTracker.testThreadStarted(getTestName());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(4096)
             .locking().concurrencyLevel(2048)
             .invocationBatching().enable()
             .persistence().passivation(false)
             .addSoftIndexFileStore().shared(false).preload(true);
      amendConfiguration(builder);
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.globalState().enable().persistentLocation(tmpDirectory);

      CyclicBarrier barrier = new CyclicBarrier(2);
      AtomicReference<Thread> putThread = new AtomicReference<>();
      Future<Void> putFuture;
      try (EmbeddedCacheManager cm = new DefaultCacheManager(globalBuilder.build())) {
         cm.defineConfiguration("leak", builder.build());
         final Cache<Object, Object> c = cm.getCache("leak");
         c.put("key1", "value1");

         putFuture = fork(() -> {
            assertSame(this, DUMMY_THREAD_LOCAL.get());
            putThread.set(Thread.currentThread());

            Cache<Object, Object> c1 = cm.getCache("leak");
            c1.put("key2", "value2");
            c1 = null;

            // Let the main thread know it can check for thread locals
            barrier.await(10, TimeUnit.SECONDS);

            // Wait for the main thread to finish the check
            barrier.await(10, TimeUnit.SECONDS);
         });

         c.put("key3", "value3");

         // Sync with the forked thread after cache.put() returns
         barrier.await(10, TimeUnit.SECONDS);
      }

      // The cache manager is stopped and the forked thread is blocked after the operation
      Map<Class<?>, Object> mainThreadLeaks = findThreadLocalLeaks(Thread.currentThread());
      assertEquals(Collections.emptySet(), mainThreadLeaks.keySet());

      Map<Class<?>, Object> forkThreadLeaks = findThreadLocalLeaks(putThread.get());
      assertEquals(Collections.singleton(DUMMY_THREAD_LOCAL.getClass()), forkThreadLeaks.keySet());

      // Let the put thread finish
      barrier.await(10, TimeUnit.SECONDS);

      // Check for any exceptions
      putFuture.get(10, TimeUnit.SECONDS);
   }

   protected void amendConfiguration(ConfigurationBuilder builder) {
      // To be overridden by subclasses
   }

   private Map<Class<?>, Object> findThreadLocalLeaks(Thread thread) throws Exception {
      // Get a reference to the thread locals table of the current thread
      Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
      threadLocalsField.setAccessible(true);
      Object threadLocalTable = threadLocalsField.get(thread);

      // Get a reference to the array holding the thread local variables inside the
      // ThreadLocalMap of the current thread
      Class<?> threadLocalMapClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
      Field tableField = threadLocalMapClass.getDeclaredField("table");
      tableField.setAccessible(true);
      Object table;
      try {
         table = tableField.get(threadLocalTable);
      } catch (NullPointerException e) {
         // Ignore
         return null;
      }

      Class<?> entryClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap$Entry");
      Field valueField = entryClass.getDeclaredField("value");
      valueField.setAccessible(true);

      Map<Class<?>, Object> threadLocals = new HashMap<>();
      for (int i=0; i < Array.getLength(table); i++) {
         // Each entry in the table array of ThreadLocalMap is an Entry object
         // representing the thread local reference and its value
         Reference<ThreadLocal<?>> entry = (Reference<ThreadLocal<?>>) Array.get(table, i);
         if (entry != null) {
            // Get a reference to the thread local object
            ThreadLocal<?> threadLocal = entry.get();
            Object value = valueField.get(entry);
            if (threadLocal != null) {
               if (filterThreadLocals(threadLocal, value) && !ACCEPTED_THREAD_LOCALS.contains(threadLocal.getClass().getCanonicalName())) {
                  log.error("Thread local leak: " + threadLocal);
                  threadLocals.put(threadLocal.getClass(), value);
               }
            } else {
               log.warn("Thread local is not accessible, but it wasn't removed either: " + value);
            }
         }
      }

      return threadLocals;
   }

   private boolean filterThreadLocals(ThreadLocal<?> tl, Object value) {
      String tlClassName = tl.getClass().getName();
      String valueClassName = value != null ? value.getClass().getName() : "";
      log.tracef("Checking thread-local %s = %s", tlClassName, valueClassName);
      if (!THREAD_LOCAL_FILTER.matcher(tlClassName).find()
            && !THREAD_LOCAL_FILTER.matcher(valueClassName).find()) {
         return false;
      }
      return !ACCEPTED_THREAD_LOCALS.contains(tlClassName) && !ACCEPTED_THREAD_LOCALS.contains(valueClassName);
   }

}
