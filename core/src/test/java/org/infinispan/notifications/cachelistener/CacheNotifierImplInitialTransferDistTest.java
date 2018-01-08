package org.infinispan.notifications.cachelistener;

import static org.infinispan.test.Mocks.invokeAndReturnMock;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;


@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplInitialTransferDistTest")
public class CacheNotifierImplInitialTransferDistTest extends MultipleCacheManagersTest {
   private final String CACHE_NAME = "DistInitialTransferListener";
   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
   }

   private enum Operation {
      PUT(Event.Type.CACHE_ENTRY_MODIFIED), CREATE(Event.Type.CACHE_ENTRY_CREATED), REMOVE(Event.Type.CACHE_ENTRY_REMOVED) {
         @Override
         public <K, V> Object perform(Cache<K, V> cache, K key, V value) {
            return cache.remove(key);
         }
      };

      private final Event.Type type;

      Operation(Event.Type type) {
         this.type = type;
      }

      public Event.Type getType() {
         return type;
      }

      public <K, V> Object perform(Cache<K, V> cache, K key, V value) {
         return cache.put(key, value);
      }
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testSimpleCacheStartingNonClusterListener() {
//      testSimpleCacheStarting(new StateListenerNotClustered());
//   }

   public void testSimpleCacheStartingClusterListener() {
      testSimpleCacheStarting(new StateListenerClustered());
   }

   private void testSimpleCacheStarting(final StateListener<String, String> listener) {
      final Map<String, String> expectedValues = new HashMap<>(10);
      Cache<String, String> cache = cache(0, CACHE_NAME);
      populateCache(cache, expectedValues);

      cache.addListener(listener);
      try {
         verifyEvents(isClustered(listener), listener, expectedValues);
      } finally {
         cache.removeListener(listener);
      }
   }

   private void populateCache(Cache<String, String> cache, Map<String, String> expectedValues) {
      for (int i = 0; i < 10; i++) {
         String key = "key-" + i;
         String value = "value-" + i;
         expectedValues.put(key, value);
         cache.put(key, value);
      }
   }

   private void verifyEvents(boolean isClustered, StateListener<String, String> listener,
                            Map<String, String> expected) {
      assertEquals(listener.events.size(), isClustered ? expected.size() : expected.size() * 2);
      boolean isPost = true;
      for (CacheEntryEvent<String, String> event : listener.events) {
         // Even checks means it will be post and have a value - note we force every check to be
         // even for clustered since those should always be post
         if (!isClustered) {
            isPost = !isPost;
         }

         assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
         assertTrue(expected.containsKey(event.getKey()));
         assertEquals(event.isPre(), !isPost);
         if (isPost) {
            assertEquals(event.getValue(), expected.get(event.getKey()));
         } else {
            assertNull(event.getValue());
         }
      }
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganButNotIteratedValueYetNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.CREATE, false);
//   }

   public void testCreateAfterIterationBeganButNotIteratedValueYetNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.CREATE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganButNotIteratedValueYetOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.CREATE, true);
//   }

   public void testCreateAfterIterationBeganButNotIteratedValueYetOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.CREATE, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganButNotIteratedValueYetNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.PUT, false);
//   }

   public void testModificationAfterIterationBeganButNotIteratedValueYetNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.PUT, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganButNotIteratedValueYetOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.PUT, true);
//   }

   public void testModificationAfterIterationBeganButNotIteratedValueYetOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.PUT, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganButNotIteratedValueYetNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.REMOVE, false);
//   }

   public void testRemoveAfterIterationBeganButNotIteratedValueYetNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.REMOVE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganButNotIteratedValueYetOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
//      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerNotClustered(), Operation.REMOVE, true);
//   }

   public void testRemoveAfterIterationBeganButNotIteratedValueYetOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException {
      testModificationAfterIterationBeganButNotIteratedValueYet(new StateListenerClustered(), Operation.REMOVE, true);
   }

   /**
    * This test is to verify that the modification event replaces the current value for the key
    */
   private void testModificationAfterIterationBeganButNotIteratedValueYet(final StateListener<String, String> listener,
                                                                          Operation operation, boolean shouldBePrimaryOwner)
         throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      final Map<String, String> expectedValues = new HashMap<>(10);
      final Cache<String, String> cache = cache(0, CACHE_NAME);
      populateCache(cache, expectedValues);

      final CheckPoint checkPoint = new CheckPoint();

      AsyncInterceptorChain chain = mockEntrySet(cache, (mock, real, additional) ->
              doAnswer(i -> {
                 // Wait for main thread to sync up
                 checkPoint.trigger("pre_retrieve_entry_invoked");
                 // Now wait until main thread lets us through
                 checkPoint.awaitStrict("pre_retrieve_entry_released", 10, TimeUnit.SECONDS);
                 return invokeAndReturnMock(i, real);
              }).when(mock).iterator());
      try {
         String keyToChange = findKeyBasedOnOwnership("key-to-change",
                                                      cache.getAdvancedCache().getDistributionManager()
                                                           .getCacheTopology(),
                                                      shouldBePrimaryOwner);
         String value = prepareOperation(operation, cache, keyToChange);
         if (value != null) {
            expectedValues.put(keyToChange, value);
         }

         Future<Void> future = fork(() -> {
            cache.addListener(listener);
            return null;
         });

         checkPoint.awaitStrict("pre_retrieve_entry_invoked", 10000, TimeUnit.SECONDS);

         operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever("pre_retrieve_entry_released");

         future.get(10, TimeUnit.SECONDS);

         verifyEvents(isClustered(listener), listener, expectedValues);
      } finally {
         TestingUtil.replaceComponent(cache, AsyncInterceptorChain.class, chain, true);
         cache.removeListener(listener);
      }
   }

   private String prepareOperation(Operation operation, Cache<String, String> cache, String keyToChange) {
      String value;
      switch (operation) {
         case CREATE:
            value = "new-value";
            break;
         case PUT:
            cache.put(keyToChange, "initial-value");
            value = "changed-value";
            break;
         case REMOVE:
            cache.put(keyToChange, "initial-value");
            value = null;
            break;
         default:
            throw new IllegalArgumentException("Unsupported Operation provided " + operation);
      }
      return value;
   }

   /**
    * This test is to verify that the modification event is sent after the creation event is done
    */
   private void testModificationAfterIterationBeganAndCompletedSegmentValueOwner(final StateListener<String, String> listener,
                                                                         Operation operation,
                                                                         boolean shouldBePrimaryOwner)
         throws IOException, InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      final Map<String, String> expectedValues = new HashMap<>(10);
      final Cache<String, String> cache = cache(0, CACHE_NAME);
      populateCache(cache, expectedValues);

      CheckPoint checkPoint = new CheckPoint();

      AsyncInterceptorChain chain = mockEntrySet(cache, (mock, real, additional) -> {
         doAnswer(i -> {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_close_iter_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_close_iter_released", 10, TimeUnit.SECONDS);
            return invokeAndReturnMock(i, real);
         }).when(mock).close();
      });

      try {
         String keyToChange = findKeyBasedOnOwnership("key-to-change",
                                                      cache.getAdvancedCache().getDistributionManager()
                                                           .getCacheTopology(),
                                                      shouldBePrimaryOwner);
         String value = prepareOperation(operation, cache, keyToChange);
         if (cache.get(keyToChange) != null) {
            expectedValues.put(keyToChange, cache.get(keyToChange));
         }

         Future<Void> future = fork(() -> {
            cache.addListener(listener);
            return null;
         });

         checkPoint.awaitStrict("pre_close_iter_invoked", 10, TimeUnit.SECONDS);

         Object oldValue = operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever("pre_close_iter_released");

         future.get(10, TimeUnit.SECONDS);

         boolean isClustered = isClustered(listener);

         // We should have 1 or 2 (local) events due to the modification coming after we iterated on it.  Note the value
         // isn't brought up until the iteration is done
         assertEquals(listener.events.size(), isClustered ? expectedValues.size() + 1 : (expectedValues.size() + 1) * 2);


         // Assert the first 10/20 since they should all be from iteration - this may not work since segments complete earlier..
         boolean isPost = true;
         int position = 0;
         for (; position < (isClustered ? expectedValues.size() : expectedValues.size() * 2); ++position) {
            // Even checks means it will be post and have a value - note we force every check to be
            // even for clustered since those should always be post
            if (!isClustered) {
               isPost = !isPost;
            }

            CacheEntryEvent event = listener.events.get(position);

            assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
            assertTrue(expectedValues.containsKey(event.getKey()));
            assertEquals(event.isPre(), !isPost);
            if (isPost) {
               assertEquals(event.getValue(), expectedValues.get(event.getKey()));
            } else {
               assertNull(event.getValue());
            }
         }


         // We should have 2 extra events at the end which are our modifications
         if (isClustered) {
            CacheEntryEvent<String, String> event = listener.events.get(position);
            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), false);
            assertEquals(event.getKey(), keyToChange);
            assertEquals(event.getValue(), value);
         } else {
            CacheEntryEvent<String, String> event = listener.events.get(position);
            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), true);
            assertEquals(event.getKey(), keyToChange);
            assertEquals(event.getValue(), oldValue);

            event = listener.events.get(position + 1);
            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), false);
            assertEquals(event.getKey(), keyToChange);
            assertEquals(event.getValue(), value);
         }
      } finally {
         TestingUtil.replaceComponent(cache, AsyncInterceptorChain.class, chain, true);
         cache.removeListener(listener);
      }
   }

   private String findKeyBasedOnOwnership(String keyPrefix, LocalizedCacheTopology cacheTopology, boolean shouldBePrimaryOwner) {
      for (int i = 0; i < 1000; i++) {
         String key = keyPrefix + i;
         boolean isPrimaryOwner = cacheTopology.getDistribution(key).isPrimary();
         if (isPrimaryOwner == shouldBePrimaryOwner) {
            if (shouldBePrimaryOwner) {
               log.debugf("Found key %s with primary owner %s, segment %d", key, cacheTopology.getLocalAddress(),
                          cacheTopology.getSegment(key));
            } else {
               log.debugf("Found key %s with primary owner != %s, segment %d", key, cacheTopology.getLocalAddress(),
                          cacheTopology.getSegment(key));
            }
            return key;
         }
      }
      throw new RuntimeException("No key could be found for owner, this may be a bug in test or really bad luck!");
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganAndCompletedSegmentValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.CREATE, false);
//   }

   public void testCreateAfterIterationBeganAndCompletedSegmentValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.CREATE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganAndCompletedSegmentValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.CREATE, true);
//   }

   public void testCreateAfterIterationBeganAndCompletedSegmentValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.CREATE, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganAndCompletedSegmentValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.PUT, false);
//   }

   public void testModificationAfterIterationBeganAndCompletedSegmentValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.PUT, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganAndCompletedSegmentValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.PUT, true);
//   }

   public void testModificationAfterIterationBeganAndCompletedSegmentValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.PUT, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganAndCompletedSegmentValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.REMOVE, false);
//   }

   public void testRemoveAfterIterationBeganAndCompletedSegmentValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.REMOVE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganAndCompletedSegmentValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerNotClustered(), Operation.REMOVE, true);
//   }

   public void testRemoveAfterIterationBeganAndCompletedSegmentValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testModificationAfterIterationBeganAndCompletedSegmentValueOwner(new StateListenerClustered(), Operation.REMOVE, true);
   }

   protected void testIterationBeganAndSegmentNotComplete(final StateListener<String, String> listener,
                                                          Operation operation, boolean shouldBePrimaryOwner)
         throws TimeoutException, InterruptedException, ExecutionException {
      final Map<String, String> expectedValues = new HashMap<>(10);
      final Cache<String, String> cache = cache(0, CACHE_NAME);
      populateCache(cache, expectedValues);

      String keyToChange = findKeyBasedOnOwnership("key-to-change-",
                                                   cache.getAdvancedCache().getDistributionManager().getCacheTopology(),
                                                         shouldBePrimaryOwner);
      String value = prepareOperation(operation, cache, keyToChange);
      if (cache.get(keyToChange) != null) {
         expectedValues.put(keyToChange, cache.get(keyToChange));
      }

      CheckPoint checkPoint = new CheckPoint();
      int segmentToUse = cache.getAdvancedCache().getDistributionManager().getCacheTopology().getSegment(keyToChange);

      // do the operation, which should put it in the queue.
      ClusterCacheNotifier notifier = waitUntilClosingSegment(cache, segmentToUse, checkPoint);

      Future<Void> future = fork(() -> {
         cache.addListener(listener);
         return null;
      });

      try {
         checkPoint.awaitStrict("pre_complete_segment_invoked", 10, TimeUnit.SECONDS);
         Object oldValue = operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever("pre_complete_segment_released");

         future.get(10, TimeUnit.SECONDS);

         boolean isClustered = isClustered(listener);

         // We should have 1 or 2 (local) events due to the modification coming after we iterated on it.  Note the value
         // isn't brought up until the iteration is done
         assertEquals(listener.events.size(), isClustered ? expectedValues.size() + 1 : (expectedValues.size() + 1) * 2);

         // If it is clustered, then the modify can occur in the middle.  In non clustered we have to block all events
         // just in case of tx event listeners (ie. tx start/tx end would have to wrap all events) and such so we can't
         // release them early.  The cluster listeners aren't affected by transaction since it those are not currently
         // supported
         if (isClustered) {

            CacheEntryEvent event = null;
            boolean foundEarlierCreate = false;
            // We iterate backwards so we only have to do it once
            for (int i = listener.events.size() - 1; i >= 0; --i) {
               CacheEntryEvent currentEvent = listener.events.get(i);
               if (currentEvent.getKey().equals(keyToChange) && operation.getType() == currentEvent.getType()) {
                  if (event == null) {
                     event = currentEvent;
                     // We can remove safely since we are doing backwards counter as well
                     listener.events.remove(i);

                     // If it is a create there is no previous create
                     if (operation.getType() == Event.Type.CACHE_ENTRY_CREATED) {
                        foundEarlierCreate = true;
                        break;
                     }
                  } else {
                     fail("There should only be a single event in the event queue!");
                  }
               } else if (event != null && (foundEarlierCreate = event.getKey().equals(currentEvent.getKey()))) {
                  break;
               }
            }
            // This should have been set
            assertTrue(foundEarlierCreate, "There was no matching create event for key " + event.getKey());

            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), false);
            assertEquals(event.getValue(), value);
         }

         // Assert the first 10/20 since they should all be from iteration - this may not work since segments complete earlier..
         boolean isPost = true;
         int position = 0;
         for (; position < (isClustered ? expectedValues.size() : expectedValues.size() * 2); ++position) {
            // Even checks means it will be post and have a value - note we force every check to be
            // even for clustered since those should always be post
            if (!isClustered) {
               isPost = !isPost;
            }

            CacheEntryEvent<String, String> event = listener.events.get(position);

            assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
            assertTrue(expectedValues.containsKey(event.getKey()));
            assertEquals(event.isPre(), !isPost);
            if (isPost) {
               assertEquals(event.getValue(), expectedValues.get(event.getKey()));
            } else {
               assertNull(event.getValue());
            }
         }


         // We should have 2 extra events at the end which are our modifications
         if (!isClustered) {
            CacheEntryEvent<String, String> event = listener.events.get(position);
            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), true);
            assertEquals(event.getKey(), keyToChange);
            assertEquals(event.getValue(), oldValue);

            event = listener.events.get(position + 1);
            assertEquals(event.getType(), operation.getType());
            assertEquals(event.isPre(), false);
            assertEquals(event.getKey(), keyToChange);
            assertEquals(event.getValue(), value);
         }
      } finally {
         cache.removeListener(listener);
         TestingUtil.replaceComponent(cache, CacheNotifier.class, notifier, true);
         TestingUtil.replaceComponent(cache, ClusterCacheNotifier.class, notifier, true);
      }
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganAndSegmentNotCompleteValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.CREATE, false);
//   }

   public void testCreateAfterIterationBeganAndSegmentNotCompleteValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.CREATE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testCreateAfterIterationBeganAndSegmentNotCompleteValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.CREATE, true);
//   }

   public void testCreateAfterIterationBeganAndSegmentNotCompleteValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.CREATE, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganAndSegmentNotCompleteValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.PUT, false);
//   }

   public void testModificationAfterIterationBeganAndSegmentNotCompleteValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.PUT, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testModificationAfterIterationBeganAndSegmentNotCompleteValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.PUT, true);
//   }

   public void testModificationAfterIterationBeganAndSegmentNotCompleteValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.PUT, true);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganAndSegmentNotCompleteValueNonOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.REMOVE, false);
//   }

   public void testRemoveAfterIterationBeganAndSegmentNotCompleteValueNonOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.REMOVE, false);
   }

//   TODO: commented out until local listners support includeCurrentState
//   public void testRemoveAfterIterationBeganAndSegmentNotCompleteValueOwnerNotClustered()
//         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
//      testIterationBeganAndSegmentNotComplete(new StateListenerNotClustered(), Operation.REMOVE, true);
//   }

   public void testRemoveAfterIterationBeganAndSegmentNotCompleteValueOwnerClustered()
         throws InterruptedException, BrokenBarrierException, TimeoutException, ExecutionException, IOException {
      testIterationBeganAndSegmentNotComplete(new StateListenerClustered(), Operation.REMOVE, true);
   }

   private boolean isClustered(StateListener listener) {
      return listener.getClass().getAnnotation(Listener.class).clustered();
   }

   protected static abstract class StateListener<K, V> {
      final List<CacheEntryEvent<K, V>> events = Collections.synchronizedList(new ArrayList<>());
      private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      public void onCacheNotification(CacheEntryEvent<K, V> event) {
         log.tracef("Received event: %s", event);
         events.add(event);
      }
   }
   @Listener(includeCurrentState = true, clustered = false)
   private static class StateListenerNotClustered extends StateListener<String, String> {

   }

   @Listener(includeCurrentState = true, clustered = true)
   private static class StateListenerClustered extends StateListener<String, String> {

   }


   protected ClusterCacheNotifier waitUntilClosingSegment(final Cache<?, ?> cache, final int segment, final CheckPoint checkPoint) {
      ClusterCacheNotifier realNotifier = TestingUtil.extractComponent(cache, ClusterCacheNotifier.class);
      ConcurrentMap<UUID, QueueingSegmentListener> listeningMap = new ConcurrentHashMap<UUID, QueueingSegmentListener>() {
         @Override
         public QueueingSegmentListener putIfAbsent(UUID key, QueueingSegmentListener value) {
            log.tracef("Adding segment listener %s : %s", key, value);
            final Answer<Object> listenerAnswer = AdditionalAnswers.delegatesTo(value);

            final AtomicBoolean wasLastSegment = new AtomicBoolean(false);
            QueueingSegmentListener mockListener = mock(QueueingSegmentListener.class,
                                                       withSettings().defaultAnswer(listenerAnswer));

            doAnswer(i -> {
               Set<Integer> segments = i.getArgument(0);
               log.tracef("Completed segments %s", segments);
               if (segments.contains(segment)) {
                  wasLastSegment.set(true);
               }
               return listenerAnswer.answer(i);
            }).when(mockListener).segmentCompleted(Mockito.anySet());

            doAnswer(i -> {
               Object k = i.getArgument(0);
               log.tracef("Notified for key %s", k);
               if (wasLastSegment.compareAndSet(true, false)) {
                  // Wait for main thread to sync up
                  checkPoint.trigger("pre_complete_segment_invoked");
                  // Now wait until main thread lets us through
                  checkPoint.awaitStrict("pre_complete_segment_released", 10, TimeUnit.SECONDS);
               }
               return listenerAnswer.answer(i);
            }).when(mockListener).notifiedKey(any());
            return super.putIfAbsent(key, mockListener);
         }
      };
      CacheNotifierImpl notifier = new CacheNotifierImpl(listeningMap);
      TestingUtil.replaceComponent(cache, CacheNotifier.class, notifier, true);
      TestingUtil.replaceComponent(cache, ClusterCacheNotifier.class, notifier, true);
      return realNotifier;
   }

   // This is a helper method so that subsequent calls to the stream will return a mocked instance so we
   // can keep track of invocations properly.
   protected Stream mockStream(CacheStream realStream, StreamMocking mocking) {
      CacheStream mockStream =
            mock(CacheStream.class, withSettings().defaultAnswer(i -> invokeAndReturnMock(i, realStream)));

      mocking.additionalInformation(mockStream, realStream, mocking);

      return mockStream;
   }

   private interface StreamMocking {
      void additionalInformation(Stream mockStream, Stream realStream, StreamMocking ourselves);
   }

   protected AsyncInterceptorChain mockEntrySet(final Cache<?, ?> cache, StreamMocking mocking) {
      AsyncInterceptorChain chain = TestingUtil.extractComponent(cache, AsyncInterceptorChain.class);
      AsyncInterceptorChain mockChain = spy(chain);
      doAnswer(i -> {
         CacheSet cacheSet = (CacheSet) i.callRealMethod();
         CacheSet mockSet = mock(CacheSet.class,
                                 withSettings().defaultAnswer(delegatesTo(cacheSet)));

         when(mockSet.stream()).then(j -> {
            CacheStream stream = cacheSet.stream();
            return mockStream(stream, mocking);
         });

         return mockSet;
      }).when(mockChain).invoke(any(InvocationContext.class), any(EntrySetCommand.class));
      TestingUtil.replaceComponent(cache, AsyncInterceptorChain.class, mockChain, true);
      return chain;
   }
}
