package org.infinispan.commons.util.concurrent.jdk8backported;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.LIRSEvictionPolicy;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.LIRSNode;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Node;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Recency;
import org.infinispan.commons.util.concurrent.jdk8backported.StrippedConcurrentLinkedDeque.DequeNode;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8LIRSTest")
public class BoundedEquivalentConcurrentHashMapV8LIRSTest extends BoundedEquivalentConcurrentHashMapV8BaseTest {

   @Override
   public Eviction evictionPolicy() {
      return Eviction.LIRS;
   }

   /**
    * Tests to make sure that if a bunch of cache writes happen that the LIRS entry
    * is not evicted
    */
   @SuppressWarnings("unchecked")
   public void testCacheWriteMisses() throws InterruptedException, ExecutionException, TimeoutException {
      int count = 50;
      final Map<String, Integer> bchm = createMap(count, evictionPolicy());

      final AtomicInteger threadOffset = new AtomicInteger();
      final int COUNT_PER_THREAD = 10000;
      final int THREADS = 10;

      // We insert a value first, note this will be promoted to LIR
      // Since we should have all misses this should never leave the cache!
      String keptKey = 0 + " " + 0;
      bchm.put(keptKey, 0);

      ExecutorService service = Executors.newFixedThreadPool(THREADS);
      Future<Void>[] futures = new Future[THREADS];
      for (int i = 0; i < THREADS; ++i) {
         futures[i] = service.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
               int offset = threadOffset.getAndIncrement();
               for (int i = 1; i < COUNT_PER_THREAD; i++) {
                  String keyValue = offset + " " + i;
                  bchm.put(keyValue, i);
               }
               return null;
            }
         });
      }
      service.shutdown();
      service.awaitTermination(10, TimeUnit.SECONDS);
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
      assertTrue(bchm.containsKey(keptKey));
   }

   private void verifyQueueContents(BoundedEquivalentConcurrentHashMapV8<String, String> bchm, 
         Iterator<DequeNode<LIRSNode<String, String>>> queueItr, 
         String... contents) {
      // Check the Queue to make sure we have what we are supposed to
      for (String content : contents) {
         assertTrue(queueItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = queueItr.next();
         // Queue elements should ALWAYS be HIR_RESIDENT
         assertEquals(Recency.HIR_RESIDENT, node.item.state);
         assertEquals(content, node.item.key);
         assertEquals(content, bchm.innerPeek(content));
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

      assertEquals(3, map.size());

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(map, policy.queue.new Itr(), "E");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
         assertEquals("D", node.item.key);
         assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("A", node.item.key);
         assertEquals("A", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, node.item.state);
         assertEquals("E", node.item.key);
         assertEquals("E", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("B", node.item.key);
         assertEquals("B", map.innerPeek(node.item.key));
         
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

      assertEquals(3, map.size());

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(map, policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
         assertEquals("D", node.item.key);
         assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("A", node.item.key);
         assertEquals("A", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("E", node.item.key);
         assertEquals("E", map.innerPeek(node.item.key));
         
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

      assertEquals(3, map.size());
      
      //    Stack S     Queue Q
      //   B (HIR-res)    B
      //   E (LIR)
      //    A (LIR)
      //   D (HIR-non)      
      assertEquals("B", map.get("B"));

      assertEquals(3, map.size());

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(map, policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
         assertEquals("D", node.item.key);
         assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("A", node.item.key);
         assertEquals("A", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("E", node.item.key);
         assertEquals("E", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, node.item.state);
         assertEquals("B", node.item.key);
         assertEquals("B", map.innerPeek(node.item.key));

         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 3 (a) from the paper state a - d
   public void testStateDUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      assertNull(map.put("D", "D"));

      assertEquals(3, map.size());

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      //    Stack S     Queue Q
      //    D (LIR)       B
      //   E (HIR-non)
      //    A (LIR)
      verifyQueueContents(map, policy.queue.new Itr(), "B");

      // Now check the Stack
      // NOTE: we do lazy pruning which the paper doesn't do so it isn't exactly the
      // same (this can cause some premature LIRS promotions
      {
         Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("A", node.item.key);
         assertEquals("A", map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
         assertEquals("E", node.item.key);
         assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));

         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("D", node.item.key);
         assertEquals("D", map.innerPeek(node.item.key));

         assertFalse(stackItr.hasNext());
      }
   }

   // This tests Section 3.3 example 3 (b) from the paper state a - e
   public void testStateEUpdate() {
      BoundedEquivalentConcurrentHashMapV8<String, String> map = initializeMapToPaper();
      assertNull(map.put("C", "C"));

      assertEquals(3, map.size());

      LIRSEvictionPolicy<String, String> policy =
            (LIRSEvictionPolicy<String, String>) map.evictionPolicy;

      // Queue should just have E
      verifyQueueContents(map, policy.queue.new Itr(), "C");

      Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
      assertTrue(stackItr.hasNext());
      DequeNode<LIRSNode<String, String>> node = stackItr.next();
      assertEquals(Recency.LIR_RESIDENT, node.item.state);
      assertEquals("B", node.item.key);
      assertEquals("B", map.innerPeek(node.item.key));
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
      assertEquals("D", node.item.key);
      assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.LIR_RESIDENT, node.item.state);
      assertEquals("A", node.item.key);
      assertEquals("A", map.innerPeek(node.item.key));
      
      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
      assertEquals("E", node.item.key);
      assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));

      assertTrue(stackItr.hasNext());
      node = stackItr.next();
      assertEquals(Recency.HIR_RESIDENT, node.item.state);
      assertEquals("C", node.item.key);
      assertEquals("C", map.innerPeek(node.item.key));

      assertFalse(stackItr.hasNext());
   }

   private BoundedEquivalentConcurrentHashMapV8<String, String> initializeMapToPaper() {
      // The paper uses a Lirs = 2 & Lhrs = 1 (which we get by using a size of 3)
      // floor(3 * .95) = 2
      BoundedEquivalentConcurrentHashMapV8<String, String> map = createMap(3, Eviction.LIRS);
      // Stack S    Queue Q
      //  B (LIR)
      map.put("B", "B");
      //    Stack S     Queue Q
      // deleteme (LIR)
      //    B (LIR)
      map.put("deleteme", "deleteme");
      //    Stack S     Queue Q
      //   D (HIR-res)    D
      // deleteme (LIR)
      //    B (LIR)
      map.put("D", "D");
      //    Stack S     Queue Q
      //   D (HIR-res)    D
      //    B (LIR)
      map.remove("deleteme");
      //    Stack S     Queue Q
      //    A (LIR)       D
      //   D (HIR-res)
      //    B (LIR)
      map.put("A", "A");
      //    Stack S     Queue Q
      //   E (HIR-res)    E
      //    A (LIR)
      //   D (HIR-non)
      //    B (LIR)
      map.put("E", "E");
      
      LIRSEvictionPolicy<String, String> policy = (LIRSEvictionPolicy<String, String>) map.evictionPolicy;
      
      verifyQueueContents(map, policy.queue.new Itr(), "E");
      
      // Now check the Stack
      {
         Iterator<DequeNode<LIRSNode<String, String>>> stackItr = policy.stack.new Itr();
         assertTrue(stackItr.hasNext());
         DequeNode<LIRSNode<String, String>> node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("B", node.item.key);
         assertEquals("B", map.innerPeek(node.item.key));
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_NONRESIDENT, node.item.state);
         assertEquals("D", node.item.key);
         assertEquals(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, map.innerPeek(node.item.key));
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.LIR_RESIDENT, node.item.state);
         assertEquals("A", node.item.key);
         assertEquals("A", map.innerPeek(node.item.key));
         
         assertTrue(stackItr.hasNext());
         node = stackItr.next();
         assertEquals(Recency.HIR_RESIDENT, node.item.state);
         assertEquals("E", node.item.key);
         assertEquals("E", map.innerPeek(node.item.key));
         
         assertFalse(stackItr.hasNext());
      }

      assertEquals(3, map.size());

      return map;
   }
}
