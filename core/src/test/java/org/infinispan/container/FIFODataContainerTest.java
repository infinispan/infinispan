package org.infinispan.container;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Test(groups = "unit", testName = "container.FIFODataContainerTest")
public class FIFODataContainerTest extends SimpleDataContainerTest {

   @Override
   protected DataContainer createContainer() {
      return new FIFODataContainer();
   }

   public void testOrdering() {
      long lifespan = 600000;
      long idle = 600000;
      for (int i = 0; i < 10; i++) dc.put("k" + i, "value", -1, -1);
      for (int i = 10; i < 20; i++) dc.put("k" + i, "value", lifespan, -1);
      for (int i = 20; i < 30; i++) dc.put("k" + i, "value", -1, idle);
      for (int i = 30; i < 40; i++) dc.put("k" + i, "value", lifespan, idle);

      // random visits
      Random r = new Random();
      for (int i = 0; i < 100; i++) dc.get("k" + r.nextInt(40));

      // ensure order is maintained.
      int i = 0;
      for (InternalCacheEntry ice : dc) {
         assert ice.getKey().equals("k" + i);
         if (i < 10) assert ice instanceof ImmortalCacheEntry;
         if (i >= 10 && i < 20) assert ice instanceof MortalCacheEntry;
         if (i >= 20 && i < 30) assert ice instanceof TransientCacheEntry;
         if (i >= 30 && i < 40) assert ice instanceof TransientMortalCacheEntry;
         i++;
      }
   }

   private void setInitialEntry() {
      FIFODataContainer ldc = (FIFODataContainer) dc;
      dc.put("k", "v", -1, -1);

      assert dc.size() == 1;

      FIFODataContainer.LinkedEntry tail = ldc.tail;
      FIFODataContainer.LinkedEntry head = ldc.head;
      FIFODataContainer.LinkedEntry e = ldc.head.n;

      assert head.n == e;
      assert head.p == tail;
      assert tail.n == head;
      assert tail.p == e;
      assert e.n == tail;
      assert e.p == head;
      assert !ldc.isMarkedForRemoval(e);
   }

   public void testInsertingLinks() {
      FIFODataContainer ldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert ldc.head.n == ldc.tail;
      assert ldc.tail.n == ldc.head;
      assert ldc.head.p == ldc.tail;
      assert ldc.tail.p == ldc.head;

      setInitialEntry();

      // add one more
      dc.put("k2", "v2", -1, -1);

      assert dc.size() == 2;

      FIFODataContainer.LinkedEntry tail = ldc.tail;
      FIFODataContainer.LinkedEntry head = ldc.head;
      FIFODataContainer.LinkedEntry le1 = head.n;
      FIFODataContainer.LinkedEntry le2 = le1.n;

      assert tail == le2.n;
      assert tail != le1.n;
      assert le1 != ldc.head;
      assert le2 != ldc.head;
      assert le1 != ldc.tail;
      assert le2 != ldc.tail;
      assert le1 != le2;

      assert le1.p == head;
      assert le1.n == le2;
      assert le2.p == le1;
      assert le2.n == tail;

      assert le1.e != null;
      assert le1.e.getKey().equals("k");
      assert le1.e.getValue().equals("v");

      assert le2.e != null;
      assert le2.e.getKey().equals("k2");
      assert le2.e.getValue().equals("v2");
   }

   public void testRemovingLinks() {
      FIFODataContainer aldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert aldc.head.n == aldc.tail;
      assert aldc.tail.n == aldc.head;
      assert aldc.head.p == aldc.tail;
      assert aldc.tail.p == aldc.head;

      setInitialEntry();

      dc.remove("k");

      assert dc.size() == 0;
      assert aldc.head.n == aldc.tail;
      assert aldc.tail.n == aldc.head;
      assert aldc.head.p == aldc.tail;
      assert aldc.tail.p == aldc.head;
   }

   public void testClear() {
      FIFODataContainer aldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert aldc.head.n == aldc.tail;
      assert aldc.tail.n == aldc.head;
      assert aldc.head.p == aldc.tail;
      assert aldc.tail.p == aldc.head;

      setInitialEntry();

      dc.clear();

      assert dc.size() == 0;
      assert aldc.head.n == aldc.tail;
      assert aldc.tail.n == aldc.head;
      assert aldc.head.p == aldc.tail;
      assert aldc.tail.p == aldc.head;
   }

   public void testMultithreadAccess() throws InterruptedException {
      assert dc.size() == 0;
      int NUM_THREADS = 10;
      long testDuration = 2000; // millis

      Random r = new Random();
      CountDownLatch startLatch = new CountDownLatch(1);

      Worker[] workers = new Worker[NUM_THREADS];
      for (int i = 0; i < NUM_THREADS; i++) workers[i] = new Worker("Worker-" + i, r, startLatch);
      for (Worker w : workers) w.start();

      startLatch.countDown();

      Thread.sleep(testDuration); // generate some noise

      for (Worker w : workers) w.running = false;
      for (Worker w : workers) w.join();
   }

   protected final class Worker extends Thread {
      CountDownLatch startLatch;
      Random r;
      volatile boolean running = true;

      public Worker(String name, Random r, CountDownLatch startLatch) {
         super(name);
         this.r = r;
         this.startLatch = startLatch;
      }

      @Override
      public void run() {
         try {
            startLatch.await();
         } catch (InterruptedException ignored) {
         }

         while (running) {
            try {
               sleep(r.nextInt(5) * 10);
            } catch (InterruptedException ignored) {
            }
            switch (r.nextInt(3)) {
               case 0:
                  dc.put("key" + r.nextInt(100), "value", -1, -1);
                  break;
               case 1:
                  dc.remove("key" + r.nextInt(100));
                  break;
               case 2:
                  dc.get("key" + r.nextInt(100));
                  break;
            }
         }
      }
   }
}