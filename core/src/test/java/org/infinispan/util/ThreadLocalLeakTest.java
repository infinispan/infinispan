/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.util;

import org.apache.log4j.helpers.ThreadLocalMap;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.FileCacheStoreConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.ref.Reference;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Tests whether certain cache set ups result in thread local leaks.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.ThreadLocalLeakTest")
public class ThreadLocalLeakTest extends AbstractInfinispanTest {

   private static final Pattern THREAD_LOCAL_FILTER = Pattern.compile("(org.infinispan.(?!test).*)");

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   public void testCheckThreadLocalLeaks() throws Exception {
      final ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .eviction().strategy(EvictionStrategy.LRU).maxEntries(4096)
            .locking().concurrencyLevel(2048)
            .loaders().passivation(false).shared(false).preload(true)
               .addFileCacheStore().location(tmpDirectory)
                  .fsyncMode(FileCacheStoreConfigurationBuilder.FsyncMode.PER_WRITE);

      Future<Map<String, Map<ThreadLocal<?>, Object>>> result = fork(
            new Callable<Map<String, Map<ThreadLocal<?>, Object>>>() {
         @Override
         public Map<String, Map<ThreadLocal<?>, Object>> call() throws Exception {
            Thread forkedThread = doStuffWithCache(builder);

            beforeGC();
            System.gc();
            Thread.sleep(500);
            System.gc();
            afterGC();

            // Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
            List<Thread> threadSet = Arrays.asList(Thread.currentThread(), forkedThread);
            Map<String, Map<ThreadLocal<?>, Object>> allThreadLocals = new HashMap<String, Map<ThreadLocal<?>, Object>>();
            for (Thread thread : threadSet) {
               Map<ThreadLocal<?>, Object> threadLocalLeaks = findThreadLocalLeaks(thread);
               if (threadLocalLeaks != null && !threadLocalLeaks.isEmpty())
                  allThreadLocals.put(thread.getName(), threadLocalLeaks);
            }

            return allThreadLocals;
         }
      });

      Map<String, Map<ThreadLocal<?>, Object>> allThreadLocals = result.get(30, TimeUnit.SECONDS);
      if (!allThreadLocals.isEmpty())
         throw new IllegalStateException("Thread locals still present: " + allThreadLocals);
   }

   private Thread doStuffWithCache(ConfigurationBuilder builder) {
      final EmbeddedCacheManager[] cm = {new DefaultCacheManager(
            new GlobalConfigurationBuilder().nonClusteredDefault().build(), builder.build(), true)};
      Thread forkedThread = null;
      try {
         final Cache<Object, Object> c = cm[0].getCache();
         c.put("key", "value");

         forkedThread = fork(new Runnable() {
            @Override
            public void run() {
               Cache<Object, Object> c = cm[0].getCache();
               c.put("key2", "value2");
               c = null;
               TestingUtil.sleepThread(2000);
            }
         }, false);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
      cm[0] = null;
      return forkedThread;
   }

   private void beforeGC() {
      // do nothing
   }

   private void afterGC() {
      // do nothing
   }

   private Map<ThreadLocal<?>, Object> findThreadLocalLeaks(Thread thread) throws Exception {
      // Get a reference to the thread locals table of the current thread
      Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
      threadLocalsField.setAccessible(true);
      Object threadLocalTable = threadLocalsField.get(thread);

      // Get a reference to the array holding the thread local variables inside the
      // ThreadLocalMap of the current thread
      Class threadLocalMapClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
      Field tableField = threadLocalMapClass.getDeclaredField("table");
      tableField.setAccessible(true);
      Object table = null;
      try {
         table = tableField.get(threadLocalTable);
      } catch (NullPointerException e) {
         // Ignore
         return null;
      }

      // The key to the ThreadLocalMap is a WeakReference object. The referent field of this object
      // is a reference to the actual ThreadLocal variable
      Field referentField = Reference.class.getDeclaredField("referent");
      referentField.setAccessible(true);
      Class<?> entryClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap$Entry");
      Field valueField = entryClass.getDeclaredField("value");
      valueField.setAccessible(true);

      Map<ThreadLocal<?>, Object> threadLocals = new HashMap<ThreadLocal<?>, Object>();
      for (int i=0; i < Array.getLength(table); i++) {
         // Each entry in the table array of ThreadLocalMap is an Entry object
         // representing the thread local reference and its value
         Object entry = Array.get(table, i);
         if (entry != null) {
            // Get a reference to the thread local object and remove it from the table
            ThreadLocal<?> threadLocal = (ThreadLocal<?>) referentField.get(entry);
            if (threadLocal != null) {
               if (filterThreadLocals(threadLocal)) {
                  log.error("Thread local leak: " + threadLocal);
                  threadLocals.put(threadLocal, threadLocal.get());
                  // threadLocal.remove();
               }
            } else {
               Object value = valueField.get(entry);
               log.warn("Thread local is not accessible, but it wasn't removed either: " + value);
            }
         }
      }

      return threadLocals;
   }

   private boolean filterThreadLocals(ThreadLocal<?> tl) {
      String threadLocalString = tl.toString();
      if (tl.get() == null)
         return THREAD_LOCAL_FILTER.matcher(threadLocalString).find();
      else {
         String threadLocalObjectString = tl.get().toString();
         return THREAD_LOCAL_FILTER.matcher(threadLocalString).find()
               || THREAD_LOCAL_FILTER.matcher(threadLocalObjectString).find();
      }
   }

}
