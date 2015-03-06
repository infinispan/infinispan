package org.infinispan.iteration;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.iteration.impl.EntryRequestCommand;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

/**
 * Test to verify distributed entry behavior
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedEntryRetrieverTest")
public class DistributedEntryRetrieverTest extends BaseClusteredEntryRetrieverTest {
   public DistributedEntryRetrieverTest() {
      this(false);
   }

   public DistributedEntryRetrieverTest(boolean tx) {
      super(tx, CacheMode.DIST_SYNC);
      // This is needed since we kill nodes
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   @Test
   public void verifyNodeLeavesBeforeGettingData() throws TimeoutException, InterruptedException, ExecutionException {
      Map<Object, String> values = putValueInEachCache(3);

      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_send_response_released");
      waitUntilSendingResponse(cache1, checkPoint);

      final EntryRetriever<Object, String> retriever = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new ArrayBlockingQueue<Map.Entry<Object, String>>(10);
      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Iterator<CacheEntry<Object, String>> iter = retriever.retrieveEntries(null, null, null, null);
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
         }
      });

      // Make sure the thread is waiting for the response
      checkPoint.awaitStrict("pre_send_response_invoked", 10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover
      killMember(1, CACHE_NAME);

      checkPoint.trigger("pre_send_response_released");

      future.get(10, TimeUnit.SECONDS);

      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertTrue("Entry wasn't found:" + entry, returnQueue.contains(entry));
      }
   }

   /**
    * This test is to verify proper behavior when a node dies after sending a batch to the requestor
    */
   @Test
   public void verifyNodeLeavesAfterSendingBackSomeData() throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      Map<Object, String> values = new HashMap<Object, String>();
      int chunkSize = cache0.getCacheConfiguration().clustering().stateTransfer().chunkSize();
      // Now insert 10 more values than the chunk size into the node we will kill
      for (int i = 0; i < chunkSize + 10; ++i) {
         MagicKey key = new MagicKey(cache1);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      // Let the first request come through fine
      checkPoint.trigger("pre_send_response_released");
      waitUntilSendingResponse(cache1, checkPoint);

      final EntryRetriever<Object, String> retriever = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<Map.Entry<Object, String>>();
      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Iterator<CacheEntry<Object, String>> iter = retriever.retrieveEntries(new NoOpFilterConverterWithDependencies<Object, String>(), null, null, null);
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
         }
      });

      // Now wait for them to send back first results
      checkPoint.awaitStrict("post_send_response_invoked", 10, TimeUnit.SECONDS);

      // We should get a value now, note all values are currently residing on cache1 as primary
      Map.Entry<Object, String> value = returnQueue.poll(10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover
      killMember(1, CACHE_NAME);

      future.get(10, TimeUnit.SECONDS);

      for (Map.Entry<Object, String> entry : values.entrySet()) {
         assertTrue("Entry wasn't found:" + entry, returnQueue.contains(entry) || entry.equals(value));
      }
   }

   @Test
   public void waitUntilProcessingResults() throws TimeoutException, InterruptedException, ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      Map<Object, String> values = new HashMap<Object, String>();
      for (int i = 0; i < 501; ++i) {
         MagicKey key = new MagicKey(cache1);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_receive_response_released");
      waitUntilStartOfProcessingResult(cache0, checkPoint);

      final EntryRetriever<Object, String> retriever = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<Map.Entry<Object, String>>();
      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Iterator<CacheEntry<Object, String>> iter = retriever.retrieveEntries(null, null, null, null);
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
         }
      });

      // Now wait for them to send back first results but don't let them process
      checkPoint.awaitStrict("pre_receive_response_invoked", 10, TimeUnit.SECONDS);

      // Now let them process the results
      checkPoint.triggerForever("pre_receive_response_released");

      // Now kill the cache - we should recover and get appropriate values
      killMember(1, CACHE_NAME);

      future.get(10, TimeUnit.SECONDS);


      ConsistentHash hash = cache0.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class).getReadConsistentHash();
      Map<Integer, Set<Map.Entry<Object, String>>> expected = generateEntriesPerSegment(hash, values.entrySet());
      Map<Integer, Set<Map.Entry<Object, String>>> answer = generateEntriesPerSegment(hash, returnQueue);

      for (Map.Entry<Integer, Set<Map.Entry<Object, String>>> entry : expected.entrySet()) {
         assertEquals("Segment " + entry.getKey() + " had a mismatch", answer.get(entry.getKey()), entry.getValue());
      }
   }

   @Test
   public void testNodeLeavesWhileIteratingOverContainerCausingRehashToLoseValues() throws TimeoutException,
                                                                                           InterruptedException,
                                                                                           ExecutionException {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      // Add an extra so that when we remove 1 it means not all the values will be on 1 node
      addClusterEnabledCacheManager(builderUsed);

      // put a lot of entries in cache0, so that when a node goes down it will lose some
      Map<Object, String> values = new HashMap<Object, String>();
      for (int i = 0; i < 501; ++i) {
         MagicKey key = new MagicKey(cache0);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_iterator_released");
      waitUntilDataContainerWillBeIteratedOn(cache0, checkPoint);

      final EntryRetriever<Object, String> retriever = cache2.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<Map.Entry<Object, String>>();
      Future<Void> future = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            Iterator<CacheEntry<Object, String>> iter = retriever.retrieveEntries(null, null, null, null);
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
         }
      });

      // Now wait for them to send back first results but don't let them process
      checkPoint.awaitStrict("pre_iterator_invoked", 10, TimeUnit.SECONDS);

      // Now kill the cache - we should recover and get appropriate values
      killMember(1, CACHE_NAME);

      // Now let them process the results
      checkPoint.triggerForever("pre_iterator_released");

      future.get(10, TimeUnit.SECONDS);


      ConsistentHash hash = cache0.getAdvancedCache().getComponentRegistry().getComponent(DistributionManager.class).getReadConsistentHash();
      Map<Integer, Set<Map.Entry<Object, String>>> expected = generateEntriesPerSegment(hash, values.entrySet());
      Map<Integer, Set<Map.Entry<Object, String>>> answer = generateEntriesPerSegment(hash, returnQueue);

      for (Map.Entry<Integer, Set<Map.Entry<Object, String>>> entry : expected.entrySet()) {
         assertEquals("Segment " + entry.getKey() + " had a mismatch", answer.get(entry.getKey()), entry.getValue());
      }
   }

   @Test
   public void testLocallyForcedEntryRetriever() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      Map<Object, String> values = new HashMap<Object, String>();
      for (int i = 0; i < 501; ++i) {
         switch (i % 3) {
            case 0:
               MagicKey key = new MagicKey(cache0);
               cache0.put(key, key.toString());
               values.put(key, key.toString());
               break;
            case 1:
               // Force it so only cache0 has it's primary owned keys
               key = new MagicKey(cache1, cache2);
               cache1.put(key, key.toString());
               break;
            case 2:
               // Force it so only cache0 has it's primary owned keys
               key = new MagicKey(cache2, cache1);
               cache2.put(key, key.toString());
               break;
            default:
               fail("Unexpected switch case!");
         }
      }

      final EntryRetriever<Object, String> retriever = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            EntryRetriever.class);

      int count = 0;
      try (CloseableIterator<CacheEntry<Object, String>> iter = retriever.retrieveEntries(null, null,
                                                                            EnumSet.of(Flag.CACHE_MODE_LOCAL), null)) {
         while (iter.hasNext()) {
            CacheEntry<Object, String> entry = iter.next();
            String cacheValue = cache0.get(entry.getKey());
            assertNotNull(cacheValue);
            assertEquals(cacheValue, entry.getValue());
            count++;
         }
      }

      assertEquals(values.size(), count);
   }

   private Map<Integer, Set<Map.Entry<Object, String>>> generateEntriesPerSegment(ConsistentHash hash, Iterable<Map.Entry<Object, String>> entries) {
      Map<Integer, Set<Map.Entry<Object, String>>> returnMap = new HashMap<Integer, Set<Map.Entry<Object, String>>>();

      for (Map.Entry<Object, String> value : entries) {
         int segment = hash.getSegment(value.getKey());
         Set<Map.Entry<Object, String>> set = returnMap.get(segment);
         if (set == null) {
            set = new HashSet<Map.Entry<Object, String>>();
            returnMap.put(segment, set);
         }
         set.add(new ImmortalCacheEntry(value.getKey(), value.getValue()));
      }
      return returnMap;
   }

   protected RpcManager waitUntilSendingResponse(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      RpcManager rpc = TestingUtil.extractComponent(cache, RpcManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(rpc);
      RpcManager mockManager = mock(RpcManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_send_response_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_send_response_released", 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_send_response_invoked");
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_send_response_released", 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockManager).invokeRemotely(anyCollectionOf(Address.class), any(EntryRequestCommand.class),
                                          any(RpcOptions.class));
      TestingUtil.replaceComponent(cache, RpcManager.class, mockManager, true);
      return rpc;
   }

   protected EntryRetriever waitUntilStartOfProcessingResult(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      EntryRetriever rpc = TestingUtil.extractComponent(cache, EntryRetriever.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(rpc);
      EntryRetriever mockRetriever = mock(EntryRetriever.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_receive_response_invoked");
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_receive_response_released", 10, TimeUnit.SECONDS);

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               // Wait for main thread to sync up
               checkPoint.trigger("post_receive_response_invoked");
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("post_receive_response_released", 10, TimeUnit.SECONDS);
            }
         }
      }).when(mockRetriever).receiveResponse(any(UUID.class), any(Address.class), anySetOf(Integer.class),
                                             anySetOf(Integer.class), anyCollection(), any(CacheException.class));
      TestingUtil.replaceComponent(cache, EntryRetriever.class, mockRetriever, true);
      return rpc;
   }

   protected DataContainer waitUntilDataContainerWillBeIteratedOn(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      DataContainer rpc = TestingUtil.extractComponent(cache, DataContainer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(rpc);
      DataContainer mocaContainer = mock(DataContainer.class, withSettings().defaultAnswer(forwardedAnswer));
      final AtomicInteger invocationCount = new AtomicInteger();
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            boolean waiting = false;
            if (invocationCount.getAndIncrement() == 0) {
               waiting = true;
               // Wait for main thread to sync up
               checkPoint.trigger("pre_iterator_invoked");
               // Now wait until main thread lets us through
               checkPoint.awaitStrict("pre_iterator_released", 10, TimeUnit.SECONDS);
            }

            try {
               return forwardedAnswer.answer(invocation);
            } finally {
               invocationCount.getAndDecrement();
               if (waiting) {
                  // Wait for main thread to sync up
                  checkPoint.trigger("post_iterator_invoked");
                  // Now wait until main thread lets us through
                  checkPoint.awaitStrict("post_iterator_released", 10, TimeUnit.SECONDS);
               }
            }
         }
      }).when(mocaContainer).iterator();
      TestingUtil.replaceComponent(cache, DataContainer.class, mocaContainer, true);
      return rpc;
   }
}

