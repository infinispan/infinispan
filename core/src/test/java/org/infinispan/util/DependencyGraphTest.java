package org.infinispan.util;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;


/**
 * Tests functionality in {@link org.infinispan.util.DependencyGraph}.
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(testName = "util.DependencyGraphTest", groups = "unit")
public class DependencyGraphTest {

   @Test
   public void testEmpty() throws CyclicDependencyException {
      assertTrue(new DependencyGraph().topologicalSort().isEmpty());
   }

   @Test
   public void testLinear() throws CyclicDependencyException {
      DependencyGraph<Integer> graph = new DependencyGraph<>();
      int size = 100;
      for (int i = 1; i <= size; i++) {
         graph.addDependency(i, i - 1);
      }
      List<Integer> sort = graph.topologicalSort();

      assertEquals(sort.size(), size + 1);
      assertEquals(sort.get(0), Integer.valueOf(100));
      assertEquals(sort.get(100), Integer.valueOf(0));
   }

   @Test
   public void testNonLinear() throws CyclicDependencyException {
      DependencyGraph<String> graph = new DependencyGraph<>();
      String A = "a";
      String B = "b";
      String C = "c";
      String D = "d";
      graph.addDependency(C, B);
      graph.addDependency(C, D);
      graph.addDependency(B, A);
      graph.addDependency(A, D);
      List<String> sort = graph.topologicalSort();

      assertEquals(sort, Arrays.asList(C, B, A, D));
   }

   @Test
   public void testIdempotency() throws CyclicDependencyException {
      DependencyGraph<String> g = new DependencyGraph<>();
      g.addDependency("N1", "N2");
      g.addDependency("N2", "N3");
      g.addDependency("N1", "N2");
      g.addDependency("N2", "N3");

      assertEquals(g.topologicalSort().size(), 3);
      assertEquals(g.topologicalSort(), Arrays.asList("N1", "N2", "N3"));
   }

   @Test
   public void testDependent() throws CyclicDependencyException {
      DependencyGraph<String> graph = new DependencyGraph<>();
      graph.addDependency("A", "B");
      graph.addDependency("A", "C");
      graph.addDependency("A", "D");
      graph.addDependency("D", "F");

      assertTrue(graph.hasDependent("B"));
      assertTrue(graph.hasDependent("C"));
      assertTrue(graph.hasDependent("D"));
      assertTrue(graph.hasDependent("F"));
      assertFalse(graph.hasDependent("A"));
      assertTrue(graph.getDependents("A").isEmpty());
      assertEquals(graph.getDependents("B").iterator().next(), "A");
      assertEquals(graph.getDependents("C").iterator().next(), "A");
      assertEquals(graph.getDependents("D").iterator().next(), "A");
      assertEquals(graph.getDependents("F").iterator().next(), "D");
   }

   @Test
   public void testConcurrentAccess() throws Exception {
      DependencyGraph<String> graph = new DependencyGraph<>();
      ExecutorService service = Executors.newCachedThreadPool();
      CountDownLatch startLatch = new CountDownLatch(1);
      int threads = 20;
      ArrayList<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
         futures.add(submitTask("A", "B", startLatch, service, graph));
         futures.add(submitTask("A", "C", startLatch, service, graph));
         futures.add(submitTask("A", "D", startLatch, service, graph));
         futures.add(submitTask("A", "B", startLatch, service, graph));
         futures.add(submitTask("D", "B", startLatch, service, graph));
         futures.add(submitTask("D", "C", startLatch, service, graph));
         futures.add(submitTask("C", "B", startLatch, service, graph));
      }
      startLatch.countDown();
      awaitAll(futures);

      assertEquals(graph.topologicalSort(), Arrays.asList("A", "D", "C", "B"));
   }

   @Test
   public void testRemoveDependency() throws CyclicDependencyException {
      DependencyGraph<String> g = new DependencyGraph<>();
      g.addDependency("E", "B");
      g.addDependency("E", "C");
      g.addDependency("E", "D");
      g.addDependency("B", "D");
      g.addDependency("B", "C");
      g.addDependency("C", "D");

      assertEquals(g.topologicalSort(), Arrays.asList("E", "B", "C", "D"));

      g.removeDependency("E", "B");
      g.addDependency("B", "E");

      assertEquals(g.topologicalSort(), Arrays.asList("B", "E", "C", "D"));

      g.clearAll();

      assertTrue(g.topologicalSort().isEmpty());
   }

   @Test
   public void testRemoveElement() throws CyclicDependencyException {
      DependencyGraph<String> g = new DependencyGraph<>();
      g.addDependency("E", "B");
      g.addDependency("E", "C");
      g.addDependency("E", "D");
      g.addDependency("B", "D");
      g.addDependency("B", "C");
      g.addDependency("C", "D");

      assertEquals(g.topologicalSort(), Arrays.asList("E", "B", "C", "D"));

      g.remove("C");
      assertEquals(g.topologicalSort(), Arrays.asList("E", "B", "D"));

      g.remove("B");
      assertEquals(g.topologicalSort(), Arrays.asList("E", "D"));

      g.remove("E");
      assertEquals(g.topologicalSort(), Arrays.asList("D"));

      g.remove("D");
      assertTrue(g.topologicalSort().isEmpty());
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testAddSelf() {
      new DependencyGraph<>().addDependency("N", "N");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testAdNull() {
      new DependencyGraph<>().addDependency("N", null);
   }

   @Test(expectedExceptions = CyclicDependencyException.class)
   public void testCycle() throws CyclicDependencyException {
      DependencyGraph<Object> graph = new DependencyGraph<>();
      Object o1 = new Object();
      Object o2 = new Object();
      Object o3 = new Object();
      graph.addDependency(o1, o2);
      graph.addDependency(o2, o3);
      graph.addDependency(o3, o1);
      graph.topologicalSort();
   }

   private Future<?> submitTask(final String from, final String to, final CountDownLatch waitingFor, ExecutorService onExecutor, final DependencyGraph<String> graph) {
      return onExecutor.submit(new Runnable() {
         @Override
         public void run() {
            try {
               waitingFor.await();
               graph.addDependency(from, to);
            } catch (InterruptedException ignored) {
            }
         }
      });
   }

   private void awaitAll(List<Future<?>> futures) throws Exception {
      for (Future f : futures) {
         f.get(10, TimeUnit.SECONDS);
      }
   }


}
