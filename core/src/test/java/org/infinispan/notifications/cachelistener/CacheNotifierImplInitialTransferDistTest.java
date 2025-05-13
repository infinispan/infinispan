package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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

      registerBlockingPublisher(checkPoint, cache);

      checkPoint.triggerForever(Mocks.AFTER_INVOCATION);
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

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

         checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

         operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

         future.get(10, TimeUnit.SECONDS);

         verifyEvents(isClustered(listener), listener, expectedValues);
      } finally {
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

      registerBlockingPublisher(checkPoint, cache);

      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

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

         checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 30, TimeUnit.SECONDS);

         Object oldValue = operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever(Mocks.AFTER_RELEASE);

         future.get(30, TimeUnit.SECONDS);

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
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      int segmentToUse = cache.getAdvancedCache().getDistributionManager().getCacheTopology().getSegment(keyToChange);

      // do the operation, which should put it in the queue.
      waitUntilClosingSegment(cache, segmentToUse, checkPoint);

      Future<Void> future = fork(() -> {
         cache.addListener(listener);
         return null;
      });

      try {
         checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);
         Object oldValue = operation.perform(cache, keyToChange, value);

         // Now let the iteration complete
         checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

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

   private abstract static class StateListener<K, V> {
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

   private void segmentCompletionWaiter(AtomicBoolean shouldFire, CheckPoint checkPoint)
         throws TimeoutException, InterruptedException {
      // Only 2 callers should come in here, so first will always succeed and second will always fail
      if (shouldFire.compareAndSet(false, true)) {
         log.tracef("We were first to check segment completion");
      } else {
         log.tracef("We were last to check segment completion, so notifying main thread");
         // Wait for main thread to sync up
         checkPoint.trigger("pre_complete_segment_invoked");
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_complete_segment_released", 10, TimeUnit.SECONDS);
      }
   }

   protected void waitUntilClosingSegment(final Cache<?, ?> cache, int segment, CheckPoint checkPoint) {
      ClusterPublisherManager<Object, String> spy = Mocks.replaceComponentWithSpy(cache, ClusterPublisherManager.class);

      doAnswer(invocation -> {
         SegmentPublisherSupplier<?> publisher = (SegmentPublisherSupplier<?>) invocation.callRealMethod();

         return Mocks.blockingSegmentPublisherOnElement(publisher, checkPoint,
               n -> n.isSegmentComplete() && n.completedSegment() == segment);
      }).when(spy).entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any());
   }

   private static void registerBlockingPublisher(final CheckPoint checkPoint, Cache<?, ?> cache) {
      ClusterPublisherManager<Object, String> spy = Mocks.replaceComponentWithSpy(cache, ClusterPublisherManager.class);

      doAnswer(invocation -> {
         SegmentPublisherSupplier<?> result = (SegmentPublisherSupplier<?>) invocation.callRealMethod();
         return Mocks.blockingPublisher(result, checkPoint);
      }).when(spy).entryPublisher(any(), any(), any(), anyLong(), any(), anyInt(), any());
   }
}
