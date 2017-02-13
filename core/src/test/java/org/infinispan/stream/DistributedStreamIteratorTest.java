package org.infinispan.stream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Test to verify distributed stream iterator
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedStreamIteratorTest")
public class DistributedStreamIteratorTest extends BaseClusteredStreamIteratorTest {
   public DistributedStreamIteratorTest() {
      this(false);
   }

   public DistributedStreamIteratorTest(boolean tx) {
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

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new ArrayBlockingQueue<>(10);
      Future<Void> future = fork(() -> {
         Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().iterator();
         while (iter.hasNext()) {
            Map.Entry<Object, String> entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
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

      Map<Object, String> values = new HashMap<>();
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

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
         Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().iterator();
         while (iter.hasNext()) {
            Map.Entry<Object, String> entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
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

      Map<Object, String> values = new HashMap<>();
      for (int i = 0; i < 501; ++i) {
         MagicKey key = new MagicKey(cache1);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_receive_response_released");
      waitUntilStartOfProcessingResult(cache0, checkPoint);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
            Iterator<Map.Entry<Object, String>> iter = cache0.entrySet().stream().iterator();
            while (iter.hasNext()) {
               Map.Entry<Object, String> entry = iter.next();
               returnQueue.add(entry);
            }
            return null;
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
      addClusterEnabledCacheManager(builderUsed).defineConfiguration(CACHE_NAME, builderUsed.build());


      // put a lot of entries in cache0, so that when a node goes down it will lose some
      Map<Object, String> values = new HashMap<>();
      for (int i = 0; i < 501; ++i) {
         MagicKey key = new MagicKey(cache0);
         cache1.put(key, key.toString());
         values.put(key, key.toString());
      }

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever("post_iterator_released");
      waitUntilDataContainerWillBeIteratedOn(cache0, checkPoint);

      final BlockingQueue<Map.Entry<Object, String>> returnQueue = new LinkedBlockingQueue<>();
      Future<Void> future = fork(() -> {
         Iterator<Map.Entry<Object, String>> iter = cache2.entrySet().stream().iterator();
         while (iter.hasNext()) {
            Map.Entry<Object, String> entry = iter.next();
            returnQueue.add(entry);
         }
         return null;
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
         assertEquals("Segment " + entry.getKey() + " had a mismatch", entry.getValue(), answer.get(entry.getKey()));
      }
   }

   @Test
   public void testLocallyForcedStream() {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      Map<Object, String> values = new HashMap<>();
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

      int count = 0;
      Iterator<Map.Entry<Object, String>> iter = cache0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).entrySet().
              stream().iterator();
      while (iter.hasNext()) {
         Map.Entry<Object, String> entry = iter.next();
         String cacheValue = cache0.get(entry.getKey());
         assertNotNull(cacheValue);
         assertEquals(cacheValue, entry.getValue());
         count++;
      }

      assertEquals(values.size(), count);
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithReHash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocally(true);
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithoutRehash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocally(false);
   }

   private void testStayLocalIfAllSegmentsPresentLocally(boolean rehashAware) throws Exception {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterStreamManager clusterStreamManager = replaceWithSpy(cache0);

      IntStream.rangeClosed(0, 499).boxed().forEach(i -> cache0.put(i, i.toString()));

      ConsistentHash ch = cache0.getAdvancedCache().getDistributionManager().getConsistentHash();
      Set<Integer> segmentsCache0 = ch.getSegmentsForOwner(cache0.getCacheManager().getAddress());

      CacheStream<Map.Entry<Object, String>> stream = cache0.entrySet().stream();
      if (!rehashAware) stream = stream.disableRehashAware();

      Map<Object, String> entries = mapFromIterator(stream.filterKeySegments(segmentsCache0).iterator());

      Map<Integer, Set<Map.Entry<Object, String>>> entriesPerSegment = generateEntriesPerSegment(ch, entries.entrySet());

      // We should not see keys from other segments, but there may be segments without any keys
      assertTrue(segmentsCache0.containsAll(entriesPerSegment.keySet()));
      verify(clusterStreamManager, never()).awaitCompletion(any(UUID.class), anyLong(), any(TimeUnit.class));
   }

   private ClusterStreamManager replaceWithSpy(Cache<?,?> cache) {
      ClusterStreamManager component = TestingUtil.extractComponent(cache, ClusterStreamManager.class);
      ClusterStreamManager clusterStreamManager = spy(component);
      TestingUtil.replaceComponent(cache, ClusterStreamManager.class, clusterStreamManager, false);
      return clusterStreamManager;
   }

   private Map<Integer, Set<Map.Entry<Object, String>>> generateEntriesPerSegment(ConsistentHash hash, Iterable<Map.Entry<Object, String>> entries) {
      Map<Integer, Set<Map.Entry<Object, String>>> returnMap = new HashMap<>();

      for (Map.Entry<Object, String> value : entries) {
         int segment = hash.getSegment(value.getKey());
         Set<Map.Entry<Object, String>> set = returnMap.get(segment);
         if (set == null) {
            set = new HashSet<>();
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
      doAnswer(invocation -> {
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
      }).when(mockManager).invokeRemotely(anyCollectionOf(Address.class), any(StreamResponseCommand.class),
                                          any(RpcOptions.class));
      TestingUtil.replaceComponent(cache, RpcManager.class, mockManager, true);
      return rpc;
   }

   protected ClusterStreamManager waitUntilStartOfProcessingResult(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      ClusterStreamManager rpc = TestingUtil.extractComponent(cache, ClusterStreamManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(rpc);
      ClusterStreamManager mockRetriever = mock(ClusterStreamManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
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
      }).when(mockRetriever).receiveResponse(any(UUID.class), any(Address.class), anyBoolean(), anySetOf(Integer.class),
              any());
      TestingUtil.replaceComponent(cache, ClusterStreamManager.class, mockRetriever, true);
      return rpc;
   }

   protected DataContainer waitUntilDataContainerWillBeIteratedOn(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      DataContainer rpc = TestingUtil.extractComponent(cache, DataContainer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(rpc);
      DataContainer mocaContainer = mock(DataContainer.class, withSettings().defaultAnswer(forwardedAnswer));
      final AtomicInteger invocationCount = new AtomicInteger();
      doAnswer(invocation -> {
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
      }).when(mocaContainer).iterator();
      TestingUtil.replaceComponent(cache, DataContainer.class, mocaContainer, true);
      return rpc;
   }
}
