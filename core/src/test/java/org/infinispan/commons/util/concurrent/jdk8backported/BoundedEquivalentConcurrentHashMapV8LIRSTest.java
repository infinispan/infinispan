package org.infinispan.commons.util.concurrent.jdk8backported;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.LIRSEvictionPolicy;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.LIRSNode;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Node;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.NullEvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Recency;
import org.infinispan.commons.util.concurrent.jdk8backported.StrippedConcurrentLinkedDeque.DequeNode;
import org.infinispan.util.EquivalentHashMapTest;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.BoundedConcurrentHashMapTest")
public class BoundedEquivalentConcurrentHashMapV8LIRSTest extends EquivalentHashMapTest {

   public void testJdkMapExpectations() {
      super.testJdkMapExpectations();
      byteArrayConditionalRemove(createStandardConcurrentMap(), false);
      byteArrayReplace(createStandardConcurrentMap(), false);
      byteArrayPutIfAbsentFail(createStandardConcurrentMap(), false);
   }

   public void testByteArrayConditionalRemove() {
      byteArrayConditionalRemove(createComparingConcurrentMap(), true);
   }

   public void testByteArrayReplace() {
      byteArrayReplace(createComparingConcurrentMap(), true);
   }

   public void testByteArrayPutIfAbsentFail() {
      byteArrayPutIfAbsentFail(createComparingConcurrentMap(), true);
   }

   protected void byteArrayConditionalRemove(
         ConcurrentMap<byte[], byte[]> map, boolean expectRemove) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] removeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] removeValue = {4, 5, 6}; // on purpose, different instance required
      if (expectRemove)
         assertTrue(String.format(
               "Expected key=%s to be removed", str(removeKey)),
               map.remove(removeKey, removeValue));
      else
         assertNull(map.get(removeKey));
   }

   protected void byteArrayReplace(
         ConcurrentMap<byte[], byte[]> map, boolean expectReplaced) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3};
      byte[] oldValue = {4, 5, 6}; // on purpose, different instance required
      byte[] newValue = {7, 8, 9}; // on purpose, different instance required
      boolean replaced = map.replace(lookupKey, oldValue, newValue);
      if (expectReplaced)
         assertTrue(String.format(
               "Expected key=%s replace of oldValue=%s with newValue=%s to work",
               str(lookupKey), str(oldValue), str(newValue)), replaced);
      else
         assertFalse(replaced);
   }

   protected void byteArrayPutIfAbsentFail(
         ConcurrentMap<byte[], byte[]> map, boolean expectFail) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] putKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = {7, 8, 9};
      byte[] previous = map.putIfAbsent(putKey, newValue);
      if (expectFail)
         assertTrue(String.format(
               "Expected putIfAbsent for key=%s to fail", str(putKey)),
               Arrays.equals(value, previous));
      else
         assertNull(previous);
   }

   protected ConcurrentMap<byte[], byte[]> createStandardConcurrentMap() {
      return new ConcurrentHashMap<byte[], byte[]>();
   }

   protected ConcurrentMap<byte[], byte[]> createComparingConcurrentMap() {
      return new BoundedEquivalentConcurrentHashMapV8<byte[], byte[]>(64, 
            Eviction.LIRS, BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), 
            EQUIVALENCE, EQUIVALENCE);
   }

   public void testLIRSCacheHits() throws InterruptedException
   {
      final int COUNT_PER_THREAD = 100000;
      final int THREADS = 10;
      final int COUNT = COUNT_PER_THREAD * THREADS;

      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         @Override
         public void onEntryChosenForEviction(Entry<Integer, Integer> entry) {
            assertEquals(COUNT, entry.getValue().intValue());
         }
      };

      final Map<Integer, Integer> bchm = createMap(COUNT + 1, Eviction.LIRS, l);

      // fill the cache (note: <=, i.e. including an entry for COUNT)
      for (int i = 0; i <= COUNT; i++)
         bchm.put(i, i);

      // start 10 threads, accessing all entries except COUNT in parallel
      Thread threads[] = new Thread[THREADS];
      for (int i = 0; i < THREADS; i++) {
         final int start = COUNT_PER_THREAD * i;
         final int end = start + COUNT_PER_THREAD;
         threads[i] = new Thread() {
            public void run() {
               for (int i = start; i < end; i++)
                  assertNotNull(bchm.get(i));
            };
         };
      }
      for (int i = 0; i < THREADS; i++)
         threads[i].start();
      for (int i = 0; i < THREADS; i++)
         threads[i].join();

      // adding one more entry must evict COUNT
      bchm.put(COUNT + 1, COUNT + 1);

      assertEquals(COUNT + 1, bchm.size());
   }

   public void testLIRSCacheMisses() throws InterruptedException, ExecutionException, TimeoutException {
      int count = 5;
      final Map<String, Integer> bchm = createMap(count, Eviction.LIRS);
      
      final AtomicInteger threadOffset = new AtomicInteger();
      final int COUNT_PER_THREAD = 200;
      final int THREADS = 2;
      ExecutorService service = Executors.newFixedThreadPool(THREADS);
      Future<Void>[] futures = new Future[2];
      for (int i = 0; i < THREADS; ++i) {
         futures[i] = service.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               int offset = threadOffset.getAndIncrement();
               for (int i = 0; i < COUNT_PER_THREAD; i++ ) {
                  String keyValue = offset + " " + i;
                  System.out.println("Calling put for " + offset + " with i: " + i);
                  bchm.put(keyValue, i);
               }
               return null;
            }
         });
      }
      service.shutdown();
      service.awaitTermination(1000, TimeUnit.SECONDS);
      for (int i = 0; i < THREADS; ++i) {
         futures[i].get(10, TimeUnit.SECONDS);
      }
      assertEquals(count, bchm.size());
      int manualCount = 0;
      for (Entry<String, Integer> entry : bchm.entrySet()) {
         if (entry.getValue() != null) {
            manualCount++;
         }
      }
      assertEquals(count, manualCount);
      assertTrue(bchm.containsKey(0 + " " + 1));
   }

   public void testLIRSHitWhenHead() {
      final BoundedEquivalentConcurrentHashMapV8<Integer, Integer> bchm = 
            createMap(2, Eviction.LIRS);
      bchm.put(0, 0);
      bchm.put(1, 1);
      
      assertEquals(0, bchm.get(0).intValue());
   }

   public void testLIRSHitWhenTail() {
      final BoundedEquivalentConcurrentHashMapV8<Integer, Integer> bchm = 
            createMap(2, Eviction.LIRS);
      bchm.put(0, 0);
      bchm.put(1, 1);
      
      assertEquals(1, bchm.get(1).intValue());
   }

   /**
    * Class that will use the provided hash code.
    * Note that this class does not override equals so only the same instance will
    * be equal to itself
    */
   static class HashCodeControlled {
      private final int hashCode;
      
      public HashCodeControlled(int hashCode) {
         this.hashCode = hashCode;
      }

      @Override
      public int hashCode() {
         return hashCode;
      }
   }

   static class HashCodeControlledPutCallable implements Callable<Void> {
      private final int hashCode;
      private final Map<HashCodeControlled, Object> map;
      private final CyclicBarrier barrier;

      public HashCodeControlledPutCallable(int hashCode, Map<HashCodeControlled, Object> map,
            CyclicBarrier barrier) {
         this.hashCode = hashCode;
         this.map = map;
         this.barrier = barrier;
      }

      @Override
      public Void call() throws Exception {
         barrier.await(10, TimeUnit.SECONDS);
         HashCodeControlled hcc = new HashCodeControlled(hashCode);
         map.put(hcc, hcc);
         return null;
      }
   }

   /**
    * Test that constantly inserts values into the map for different tables, which will
    * hopefully detect a deadlock when a new values is inserted in 1 table that has to
    * evict the value from another table
    * @throws TimeoutException 
    * @throws BrokenBarrierException 
    * @throws InterruptedException 
    */
   public void testDeadlockEvictingElementInOtherTable() throws InterruptedException,
         BrokenBarrierException, TimeoutException {
      Map<HashCodeControlled, Object> bchm = 
            createMap(2, Eviction.LIRS);
      CyclicBarrier barrier = new CyclicBarrier(3);
      ExecutorService service = Executors.newFixedThreadPool(2);
      
      
      try {
         // Now we do a bunch of puts trying to interleave them
         for (int i = 0; i < 10000; i++) {
            service.submit(new HashCodeControlledPutCallable(0, bchm, barrier));
            service.submit(new HashCodeControlledPutCallable(1, bchm, barrier));
            barrier.await(10, TimeUnit.SECONDS);
         }
      } finally {
         service.shutdownNow();
      }
   }

   private void verifyQueueContents(Iterator<DequeNode<Node<String, String>>> queueItr, 
         String... contents) {
      // Check the Queue to make sure we have what we are supposed to
      for (String content : contents) {
         assertTrue(queueItr.hasNext());
         DequeNode<Node<String, String>> node = queueItr.next();
         // Queue elements should ALWAYS be HIR_RESIDENT
         assertEquals(Recency.HIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals(content, node.item.key);
         assertEquals(content, node.item.val);
      }
      
      assertFalse(queueItr.hasNext());
   }

   public void testInit() {
      initializeMapToPaper();
   }

   // This tests Section 3.3 example 1 from the paper - state a to b
   public void testStateBUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      //    Stack S     Queue Q
      //    B (LIR)       E
      //   E (HIR-res)
      //    A (LIR)
      //   D (HIR-non)
      assertEquals("B", map.get("B"));

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(policy.queue.new Itr(), "E");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<Node<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("D", node.item.key);
         assertNull(node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("A", node.item.key);
         assertEquals("A", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("E", node.item.key);
         assertEquals("E", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("B", node.item.key);
         assertEquals("B", node.item.val);
         
         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 2 (a) from the paper state a - c
   public void testStateCUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      //    Stack S     Queue Q
      //   E (LIR)        B
      //    A (LIR)
      //   D (HIR-non)
      assertEquals("E", map.get("E"));

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<Node<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("D", node.item.key);
         assertNull(node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("A", node.item.key);
         assertEquals("A", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("E", node.item.key);
         assertEquals("E", node.item.val);
         
         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 2 (b) from the paper
   // Note this is not shown as a diagram, but according to the use case if it wasn't in
   // the stack but was in the queue we just add it to the stack.
   public void testStateC2Update() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      //    Stack S     Queue Q
      //   E (LIR)        B
      //    A (LIR)
      //   D (HIR-non)
      assertEquals("E", map.get("E"));
      //    Stack S     Queue Q
      //   B (HIR-res)    B
      //   E (LIR)
      //    A (LIR)
      //   D (HIR-non)      
      assertEquals("B", map.get("B"));

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<Node<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("D", node.item.key);
         assertNull(node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("A", node.item.key);
         assertEquals("A", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("E", node.item.key);
         assertEquals("E", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("B", node.item.key);
         assertEquals("B", node.item.val);

         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 3 (a) from the paper state a - d
   public void testStateDUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      assertNull(map.put("D", "D"));

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<Node<String, String>> node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("A", node.item.key);
         assertEquals("A", node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("E", node.item.key);
         assertEquals(null, node.item.val);

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("D", node.item.key);
         assertEquals("D", node.item.val);

         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 3 (b) from the paper state a - e
   public void testStateEUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      assertNull(map.put("C", "C"));

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(policy.queue.new Itr(), "C");

      Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
      assertTrue(stackItr.hasNext());
      DequeNode<Node<String, String>> node = stackItr.next();
      assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
      assertEquals("B", node.item.key);
      assertEquals("B", node.item.val);
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
      assertEquals("D", node.item.key);
      assertNull(node.item.val);
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
      assertEquals("A", node.item.key);
      assertEquals("A", node.item.val);
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
      assertEquals("E", node.item.key);
      assertNull(node.item.val);

      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
      assertEquals("C", node.item.key);
      assertEquals("C", node.item.val);

      assertFalse(stackItr.hasNext());
   }

   private BoundedEquivalentConcurrentHashMapV8<String, String> initializeMapToPaper() {
      // The paper uses a Lirs = 2 & Lhrs = 1 (which we get by using a size of 3)
      // floor(3 * .95) = 2
      BoundedEquivalentConcurrentHashMapV8<String, String> bchm = createMap(3, Eviction.LIRS);
      // Stack S    Queue Q
      //  B (LIR)
      bchm.put("B", "B");
      //    Stack S     Queue Q
      // deleteme (LIR)
      //    B (LIR)
      bchm.put("deleteme", "deleteme");
      //    Stack S     Queue Q
      //   D (HIR-res)    D
      // deleteme (LIR)
      //    B (LIR)
      bchm.put("D", "D");
      //    Stack S     Queue Q
      //   D (HIR-res)    D
      //    B (LIR)
      bchm.remove("deleteme");
      //    Stack S     Queue Q
      //    A (LIR)       D
      //   D (HIR-res)
      //    B (LIR)
      bchm.put("A", "A");
      //    Stack S     Queue Q
      //   E (HIR-res)    E
      //    A (LIR)
      //   D (HIR-non)
      //    B (LIR)
      bchm.put("E", "E");
      
      LIRSEvictionPolicy<String, String> policy = (LIRSEvictionPolicy<String, String>) bchm.evictionPolicy;
      
      verifyQueueContents(policy.queue.new Itr(), "E");
      
      // Now check the Stack
      {
         Iterator<DequeNode<Node<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<Node<String, String>> node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("B", node.item.key);
         assertEquals("B", node.item.val);
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("D", node.item.key);
         assertNull(node.item.val);
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("A", node.item.key);
         assertEquals("A", node.item.val);
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, ((LIRSNode)node.item.eviction).state);
         assertEquals("E", node.item.key);
         assertEquals("E", node.item.val);
         
         assertFalse(stackItr.hasNext());
      }
      
      return bchm;
   }

   private <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction) {
      return createMap(maxSize, eviction, 
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener());
   }

   private <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction, EvictionListener<? super K, ? super V> listener) {
      return new BoundedEquivalentConcurrentHashMapV8<K, V>(
            maxSize, maxSize >> 1, eviction, listener, 
            AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
   }
}
