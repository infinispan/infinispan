package org.horizon.container;

import org.horizon.container.entries.ImmortalCacheEntry;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.container.entries.MortalCacheEntry;
import org.horizon.container.entries.TransientCacheEntry;
import org.horizon.container.entries.TransientMortalCacheEntry;
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

      FIFODataContainer.Aux last = ldc.dummyEntry.prev;
      FIFODataContainer.Aux next = ldc.dummyEntry.next;
      FIFODataContainer.LinkedEntry le = next.next;
      FIFODataContainer.Aux last2 = le.next;

      assert last == last2;
      assert last != next;
      assert le != ldc.dummyEntry;
      assert le.prev == next;
      assert le.next == last;
      assert le.entry != null;
      assert le.entry.getKey().equals("k");
      assert le.entry.getValue().equals("v");
   }

   public void testInsertingLinks() {
      FIFODataContainer ldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert ldc.dummyEntry.prev == ldc.dummyEntry.next;
      assert ldc.dummyEntry.entry == null;

      setInitialEntry();

      // add one more
      dc.put("k2", "v2", -1, -1);

      assert dc.size() == 2;

      FIFODataContainer.Aux last = ldc.dummyEntry.prev;
      FIFODataContainer.Aux next = ldc.dummyEntry.next;
      FIFODataContainer.LinkedEntry le1 = next.next;
      FIFODataContainer.Aux next2 = le1.next;
      FIFODataContainer.LinkedEntry le2 = next2.next;
      FIFODataContainer.Aux last2 = le2.next;

      assert last == last2;
      assert last != next;
      assert last != next2;
      assert next != next2;
      assert le1 != ldc.dummyEntry;
      assert le2 != ldc.dummyEntry;
      assert le1 != le2;

      assert le1.prev == next;
      assert le1.next == next2;
      assert le2.prev == next2;
      assert le2.next == last;

      assert le1.entry != null;
      assert le1.entry.getKey().equals("k");
      assert le1.entry.getValue().equals("v");

      assert le2.entry != null;
      assert le2.entry.getKey().equals("k2");
      assert le2.entry.getValue().equals("v2");
   }

   public void testRemovingLinks() {
      FIFODataContainer aldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert aldc.dummyEntry.prev == aldc.dummyEntry.next;
      assert aldc.dummyEntry.entry == null;

      setInitialEntry();

      dc.remove("k");

      assert dc.size() == 0;
      assert aldc.dummyEntry.prev == aldc.dummyEntry.next;
      assert aldc.dummyEntry.entry == null;
   }

   public void testClear() {
      FIFODataContainer aldc = (FIFODataContainer) dc;
      assert dc.size() == 0;
      assert aldc.dummyEntry.prev == aldc.dummyEntry.next;
      assert aldc.dummyEntry.entry == null;

      setInitialEntry();

      dc.clear();

      assert dc.size() == 0;
      assert aldc.dummyEntry.prev == aldc.dummyEntry.next;
      assert aldc.dummyEntry.entry == null;
   }

   public void testMultithreadAccess() throws InterruptedException {
      assert dc.size() == 0;
      int NUM_THREADS = 5;
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

      assertNoStaleSpinLocks((FIFODataContainer) dc);
   }

   protected void assertNoStaleSpinLocks(FIFODataContainer fdc) {
      FIFODataContainer.SpinLock first = fdc.dummyEntry;
      FIFODataContainer.SpinLock next = fdc.dummyEntry;

      do {
         assert !next.l.get() : "Should NOT be locked!";
         if (next instanceof FIFODataContainer.Aux)
            next = ((FIFODataContainer.Aux) next).next;
         else
            next = ((FIFODataContainer.LinkedEntry) next).next;

      } while (first != next);
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
