package org.infinispan.util.concurrent;

import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

@Test(groups = "unit")
public class WatchableValueTest {
   public void testNotifier() throws InterruptedException {
      final WatchableValue vn = new WatchableValue(10);
      final List<Integer> threadsCompleted = new LinkedList<Integer>();


      Thread t1 = new Thread() {
         public void run() {
            try {
               vn.awaitValue(50);
               threadsCompleted.add(1);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      Thread t2 = new Thread() {
         public void run() {
            try {
               vn.awaitValue(50);
               threadsCompleted.add(2);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      Thread t3 = new Thread() {
         public void run() {
            try {
               vn.awaitValue(40);
               threadsCompleted.add(3);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      t1.start();
      t2.start();
      t3.start();

      Thread.sleep(100);
      assert threadsCompleted.isEmpty();
      vn.setValue(50);
      Thread.sleep(100);
      assert threadsCompleted.size() == 2;
      assert threadsCompleted.contains(1);
      assert threadsCompleted.contains(2);

      vn.setValue(40);
      Thread.sleep(100);
      assert threadsCompleted.size() == 3;
      assert threadsCompleted.contains(3);
   }
}
