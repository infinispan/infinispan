package org.infinispan.persistence.file;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Test concurrent reads, writes, and clear operations on the SingleFileStore.
 *
 * @author Dan Berindei
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileStoreStressTest")
public class SingleFileStoreStressTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "testCache";
   private static final String TIMES_STRING = "123456789_";
   private String location;

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(this.location);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      location = TestingUtil.tmpDirectory(SingleFileStoreStressTest.class);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addSingleFileStore().location(this.location).purgeOnStartup(true);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());
      return cacheManager;
   }

   public void testReadsAndWrites() throws ExecutionException, InterruptedException {
      final int writerThreads = 2;
      final int readerThreads = 2;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      final List<String> keys = populateStore(5, 0, store, marshaller);

      final CountDownLatch stopLatch = new CountDownLatch(1);
      Future[] writeFutures = new Future[writerThreads];
      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i] = fork(stopOnException(new WriteTask(store, marshaller, keys, stopLatch), stopLatch));
      }

      Future[] readFutures = new Future[readerThreads];
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i] = fork(stopOnException(new ReadTask(store, keys, false, stopLatch), stopLatch));
      }

      stopLatch.await(2, SECONDS);
      stopLatch.countDown();

      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i].get();
      }
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i].get();
      }
   }

   public void testWritesAndClear() throws ExecutionException, InterruptedException {
      final int writerThreads = 2;
      final int readerThreads = 2;
      final int numberOfKeys = 5;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      final List<String> keys = new ArrayList<>(numberOfKeys);
      for (int j = 0; j < numberOfKeys; j++) {
         String key = "key" + j;
         keys.add(key);
      }

      final CountDownLatch stopLatch = new CountDownLatch(1);
      Future[] writeFutures = new Future[writerThreads];
      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i] = fork(stopOnException(new WriteTask(store, marshaller, keys, stopLatch), stopLatch));
      }
      Future[] readFutures = new Future[readerThreads];
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i] = fork(stopOnException(new ReadTask(store, keys, true, stopLatch), stopLatch));
      }
      Future clearFuture = fork(stopOnException(new ClearTask(store, stopLatch), stopLatch));

      stopLatch.await(2, SECONDS);
      stopLatch.countDown();

      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i].get();
      }
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i].get();
      }
      clearFuture.get();
   }

   public void testSpaceOptimization() throws ExecutionException, InterruptedException {
      final int numberOfKeys = 100;
      final int times = 10;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      long [] fileSizesWithoutPurge = new long [times];
      long [] fileSizesWithPurge = new long [times];
      File file = new File(location, CACHE_NAME + ".dat");

      // Write values for all keys iteratively such that the entry size increases during each iteration
      // Also record the file size after each such iteration.
      // Since entry sizes increase during each iteration, new entries won't fit in old free entries
      for (int i = 0; i < times; i++) {
         populateStore(numberOfKeys, i, store, marshaller);
         fileSizesWithoutPurge[i] = file.length();
      }

      // Clear the store so that we can start fresh again
      store.clear();

      // Repeat the same logic as above
      // Just that, in this case we will call purge after each iteration
      ExecutorService executor = Executors.newSingleThreadExecutor(getTestThreadFactory("Purge"));
      try {
         for (int i = 0; i < times; i++) {
            populateStore(numberOfKeys, i, store, marshaller);
            // Call purge so that the entries are coalesced
            // Since this will merge and make bigger free entries available, new entries should get some free slots (unlike earlier case)
            // This should prove that the file size increases slowly
            store.purge(executor, null);
            // Give some time for the purge thread to finish
            MILLISECONDS.sleep(200);
            fileSizesWithPurge[i] = file.length();
         }
      } finally {
         executor.shutdownNow();
      }

      // Verify that file size increases slowly when the space optimization logic (implemented within store.purge()) is used
      for (int j = 2; j < times; j++) {
         assertTrue(fileSizesWithPurge[j] < fileSizesWithoutPurge[j]);
      }
   }

   public void testFileTruncation() throws ExecutionException, InterruptedException {
      final int writerThreads = 2;
      final int readerThreads = 2;
      final int numberOfKeys = 5;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      // Write a few entries into the cache
      final List<String> keys = populateStore(5, 0, store, marshaller);

      // Do some reading/writing entries with random size
      final CountDownLatch stopLatch = new CountDownLatch(1);
      Future[] writeFutures = new Future[writerThreads];
      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i] = fork(stopOnException(new WriteTask(store, marshaller, keys, stopLatch), stopLatch));
      }

      Future[] readFutures = new Future[readerThreads];
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i] = fork(stopOnException(new ReadTask(store, keys, false, stopLatch), stopLatch));
      }

      stopLatch.await(2, SECONDS);
      stopLatch.countDown();

      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i].get();
      }
      for (int i = 0; i < readerThreads; i++) {
         readFutures[i].get();
      }

      File file = new File(location, CACHE_NAME + ".dat");
      long length1 = file.length();
      store.purge(null, null);
      long length2 = file.length();
      assertTrue(String.format("Length1=%d, Length2=%d", length1, length2), length2 <= length1);

      // Write entry with size larger than any previous to ensure that it is placed at the end of the file
      String key = "key" + numberOfKeys;
      byte[] bytes = new byte[(int) store.getFileSize()];
      ByteBuffer buf = new ByteBufferImpl(bytes, 0, bytes.length);
      store.write(new MarshalledEntryImpl<>(key, buf, (ByteBuffer) null, marshaller));
      length1 = file.length();

      // Delete entry in order to guarantee that there will be space available at the end of the file to truncate
      store.delete(key);
      store.purge(null, null);
      length2 = file.length();
      assertTrue(String.format("Length1=%d, Length2=%d", length1, length2), length2 < length1);
   }

   public List<String> populateStore(int numKeys, int numPadding, SingleFileStore store, StreamingMarshaller marshaller) {
      final List<String> keys = new ArrayList<>(numKeys);
      for (int j = 0; j < numKeys; j++) {
         String key = "key" + j;
         String value = key + "_value_" + j + times(numPadding);
         keys.add(key);
         MarshalledEntryImpl entry = new MarshalledEntryImpl<>(key, value, null, marshaller);
         store.write(entry);
      }
      return keys;
   }

   public void testProcess() throws ExecutionException, InterruptedException {
      final int writerThreads = 2;
      final int numberOfKeys = 2000;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      final List<String> keys = new ArrayList<>(numberOfKeys);
      populateStoreRandomValues(numberOfKeys, store, marshaller, keys);

      final CountDownLatch stopLatch = new CountDownLatch(1);
      Future[] writeFutures = new Future[writerThreads];
      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i] = fork(stopOnException(new WriteTask(store, marshaller, keys, stopLatch), stopLatch));
      }

      Future processFuture = fork(stopOnException(new ProcessTask(store), stopLatch));

      // Stop the writers only after we finish processing
      processFuture.get();
      stopLatch.countDown();

      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i].get();
      }
   }

   public void testProcessWithNoDiskAccess() throws ExecutionException, InterruptedException {
      final int writerThreads = 2;
      final int numberOfKeys = 2000;

      Cache<String, String> cache = cacheManager.getCache(CACHE_NAME);
      final SingleFileStore store = TestingUtil.getFirstWriter(cache);
      final StreamingMarshaller marshaller = TestingUtil.extractComponentRegistry(cache).getCacheMarshaller();
      assertEquals(0, store.size());

      final List<String> keys = new ArrayList<>(numberOfKeys);
      populateStoreRandomValues(numberOfKeys, store, marshaller, keys);

      final CountDownLatch stopLatch = new CountDownLatch(1);
      Future[] writeFutures = new Future[writerThreads];
      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i] = fork(stopOnException(new WriteTask(store, marshaller, keys, stopLatch), stopLatch));
      }

      Future processFuture = fork(stopOnException(new ProcessTaskNoDiskRead(store), stopLatch));

      // Stop the writers only after we finish processing
      processFuture.get();
      stopLatch.countDown();

      for (int i = 0; i < writerThreads; i++) {
         writeFutures[i].get();
      }
   }

   private void populateStoreRandomValues(int numberOfKeys, SingleFileStore store,
                                          StreamingMarshaller marshaller, List<String> keys) {
      for (int j = 0; j < numberOfKeys; j++) {
         String key = "key" + j;
         String value = key + "_value_" + j + times(new Random().nextInt(10));
         keys.add(key);
         MarshalledEntryImpl entry = new MarshalledEntryImpl<>(key, value, null, marshaller);
         store.write(entry);
      }
   }

   private Callable<Object> stopOnException(Callable<Object> task, CountDownLatch stopLatch) {
      return new StopOnExceptionTask(task, stopLatch);
   }

   private String times(int count) {
      if (count == 0)
         return "";

      StringBuilder sb = new StringBuilder(TIMES_STRING.length() * count);
      for (int i = 0; i < count; i++) {
         sb.append(TIMES_STRING);
      }
      return sb.toString();
   }

   private class WriteTask implements Callable<Object> {
      final SingleFileStore store;
      final StreamingMarshaller marshaller;
      final List<String> keys;
      final CountDownLatch stopLatch;

      WriteTask(SingleFileStore store, StreamingMarshaller marshaller, List<String> keys, CountDownLatch stopLatch) {
         this.store = store;
         this.marshaller = marshaller;
         this.keys = keys;
         this.stopLatch = stopLatch;
      }

      @Override
      public Object call() throws Exception {
         TestResourceTracker.testThreadStarted(SingleFileStoreStressTest.this);
         Random random = new Random();
         int i = 0;
         while (stopLatch.getCount() != 0) {
            String key = keys.get(random.nextInt(keys.size()));
            String value = key + "_value_" + i + "_" + times(random.nextInt(1000) / 10);
            MarshalledEntry entry = new MarshalledEntryImpl<>(key, value, null, marshaller);
            store.write(entry);
            i++;
         }
         return null;
      }
   }

   private class ReadTask implements Callable<Object> {
      final boolean allowNulls;
      final CountDownLatch stopLatch;
      final List<String> keys;
      final SingleFileStore store;

      ReadTask(SingleFileStore store, List<String> keys, boolean allowNulls, CountDownLatch stopLatch) {
         this.allowNulls = allowNulls;
         this.stopLatch = stopLatch;
         this.keys = keys;
         this.store = store;
      }

      @Override
      public Random call() throws Exception {
         Random random = new Random();
         while (stopLatch.getCount() != 0) {
            String key = keys.get(random.nextInt(keys.size()));
            MarshalledEntry entryFromStore = store.load(key);
            if (entryFromStore == null) {
               assertTrue(allowNulls);
            } else {
               String storeValue = (String) entryFromStore.getValue();
               assertEquals(key, entryFromStore.getKey());
               assertTrue(storeValue.startsWith(key));
            }
         }
         return null;
      }
   }

   private class ClearTask implements Callable<Object> {
      final CountDownLatch stopLatch;
      final SingleFileStore store;

      ClearTask(SingleFileStore store, CountDownLatch stopLatch) {
         this.stopLatch = stopLatch;
         this.store = store;
      }

      @Override
      public Object call() throws Exception {
         File file = new File(location, CACHE_NAME + ".dat");
         assertTrue(file.exists());

         MILLISECONDS.sleep(100);
         while (stopLatch.getCount() != 0) {
            log.tracef("Clearing store, store size before = %d, file size before = %d", store.getFileSize(), file.length());
            store.clear();
            MILLISECONDS.sleep(1);
            // The store size is incremented before values are actually written, so the on-disk size should always be
            // smaller than the logical size.
            long fileSizeAfterClear = file.length();
            long storeSizeAfterClear = store.getFileSize();
            log.tracef("Cleared store, store size after = %d, file size after = %d", storeSizeAfterClear, fileSizeAfterClear);
            assertTrue("Store size " + storeSizeAfterClear + " is smaller than the file size " + fileSizeAfterClear,
                  fileSizeAfterClear <= storeSizeAfterClear);
            MILLISECONDS.sleep(100);
         }
         return null;
      }
   }

   private class ProcessTask implements Callable<Object> {
      final SingleFileStore store;

      ProcessTask(SingleFileStore store) {
         this.store = store;
      }

      @Override
      public Object call() throws Exception {
         final int NUM_PROCESS_THREADS = ProcessorInfo.availableProcessors();

         File file = new File(location, CACHE_NAME + ".dat");
         assertTrue(file.exists());

         final ExecutorService executor = Executors.newFixedThreadPool(NUM_PROCESS_THREADS, getTestThreadFactory("process"));
         try {
            final AtomicInteger count = new AtomicInteger(0);
            store.process(key -> true,
                          (marshalledEntry, taskContext) -> {
                             count.incrementAndGet();
                             Object key = marshalledEntry.getKey();
                             Object value = marshalledEntry.getValue();
                             assertEquals(key, ((String) value).substring(0, ((String) key).length()));
                          }, executor, true, true);
            log.tracef("Processed %d entries from the store", count.get());
         } finally {
            executor.shutdownNow();
         }
         return null;
      }
   }

   private class ProcessTaskNoDiskRead implements Callable<Object> {
      final SingleFileStore store;

      ProcessTaskNoDiskRead(SingleFileStore store) {
         this.store = store;
      }

      @Override
      public Object call() throws Exception {
         final int NUM_PROCESS_THREADS = ProcessorInfo.availableProcessors();

         File file = new File(location, CACHE_NAME + ".dat");
         assertTrue(file.exists());

         ExecutorService executor = Executors.newFixedThreadPool(NUM_PROCESS_THREADS, getTestThreadFactory("process"));
         try {
            final AtomicInteger count = new AtomicInteger(0);
            store.process(
                  (key) -> true,
                  (marshalledEntry, taskContext) -> {
                     count.incrementAndGet();
                     Object key = marshalledEntry.getKey();
                     assertNotNull(key);
                  },
                  executor, false, false);  // Set (value and metadata) == false so that no disk reads are performed
            log.tracef("Processed %d in-memory keys from the store", count.get());
         } finally {
            executor.shutdownNow();
         }
         return null;
      }
   }

   private class StopOnExceptionTask implements Callable<Object> {
      final CountDownLatch stopLatch;
      final Callable<Object> delegate;

      StopOnExceptionTask(Callable<Object> delegate, CountDownLatch stopLatch) {
         this.stopLatch = stopLatch;
         this.delegate = delegate;
      }

      @Override
      public Object call() throws Exception {
         try {
            return delegate.call();
         } catch (Throwable t) {
            stopLatch.countDown();
            throw new Exception(t);
         }
      }
   }
}
