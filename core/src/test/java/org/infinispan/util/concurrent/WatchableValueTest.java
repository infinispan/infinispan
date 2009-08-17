package org.infinispan.util.concurrent;

import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Test(groups = "unit", testName = "util.concurrent.WatchableValueTest")
public class WatchableValueTest {
   @Test(invocationCount = 250, skipFailedInvocations = true)
   public void testNotifier() throws InterruptedException {
      final WatchableValue vn = new WatchableValue(10);
      final List<Integer> threadsCompleted = new LinkedList<Integer>();
      final CountDownLatch threadsReady = new CountDownLatch(3), valueSet1 = new CountDownLatch(2), valueSet2 = new CountDownLatch(1);

      Thread t1 = new Thread() {
         public void run() {
            try {
               threadsReady.countDown();
               vn.awaitValue(50);
               threadsCompleted.add(1);
               valueSet1.countDown();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      Thread t2 = new Thread() {
         public void run() {
            try {
               threadsReady.countDown();
               vn.awaitValue(50);
               threadsCompleted.add(2);
               valueSet1.countDown();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      Thread t3 = new Thread() {
         public void run() {
            try {
               threadsReady.countDown();
               vn.awaitValue(40);
               threadsCompleted.add(3);
               valueSet2.countDown();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      t1.start();
      t2.start();
      t3.start();

      threadsReady.await();
      assert threadsCompleted.isEmpty();
      vn.setValue(50);
      valueSet1.await();
      assert threadsCompleted.size() == 2;
      assert threadsCompleted.contains(1);
      assert threadsCompleted.contains(2);

      vn.setValue(40);
      valueSet2.await();
      assert threadsCompleted.size() == 3;
      assert threadsCompleted.contains(3);
   }
}
