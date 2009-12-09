package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Test(groups = "unit", testName = "container.FIFODataContainerTest")
public class FIFODataContainerTest extends SimpleDataContainerTest {

   @Override
   protected DataContainer createContainer() {
      return new FIFOSimpleDataContainer(16, 1);
   }

   @Override
   protected Class<? extends InternalCacheEntry> transienttype() {
      return TransientMortalCacheEntry.class;
   }

   @Override
   protected Class<? extends InternalCacheEntry> immortaltype() {
      return MortalCacheEntry.class;
   }

   public void testOrdering() {
      long lifespan = 600000;
      long idle = 600000;
      for (int i = 0; i < 10; i++) {
         dc.put("k" + i, "value", -1, -1);
         TestingUtil.sleepThread(10);
      }
      for (int i = 10; i < 20; i++) {
         dc.put("k" + i, "value", lifespan, -1);
         TestingUtil.sleepThread(10);
      }
      for (int i = 20; i < 30; i++) {
         dc.put("k" + i, "value", -1, idle);
         TestingUtil.sleepThread(10);
      }
      for (int i = 30; i < 40; i++) {
         dc.put("k" + i, "value", lifespan, idle);
         TestingUtil.sleepThread(10);
      }

      // random visits
      Random r = new Random();
      for (int i = 0; i < 100; i++) {
         dc.get("k" + r.nextInt(40));
         TestingUtil.sleepThread(10);
      }

      // ensure order is maintained.
      int i = 0;
      for (InternalCacheEntry ice : dc) {
         assert ice.getKey().equals("k" + i);
         if (i < 10) assert ice.getClass().equals(immortaltype());
         if (i >= 10 && i < 20) assert ice.getClass().equals(mortaltype());
         if (i >= 20 && i < 30) assert ice.getClass().equals(transienttype());
         if (i >= 30 && i < 40) assert ice instanceof TransientMortalCacheEntry;
         i++;
      }
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