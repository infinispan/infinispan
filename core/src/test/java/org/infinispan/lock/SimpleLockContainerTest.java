package org.infinispan.lock;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

@Test (groups = "functional", testName = "lock.SimpleLockContainerTest")
public class SimpleLockContainerTest extends AbstractInfinispanTest {

   OwnableReentrantPerEntryLockContainer lc = new OwnableReentrantPerEntryLockContainer(1000);

   public void simpleTest() throws Exception {
      final String k1 = ab();
      final String k2 = ab2();
      assert k1 != k2 && k1.equals(k2);

      Object owner = new Object();
      lc.acquireLock(owner, k1, 0, TimeUnit.MILLISECONDS);
      assert lc.isLocked(k1);


      fork(new Runnable() {
         @Override
         public void run() {
            final Object otherOwner = new Object();
            for (int i =0; i < 10; i++) {
               try {
                  final OwnableReentrantLock ownableReentrantLock = lc.acquireLock(otherOwner, k2, 500, TimeUnit.MILLISECONDS);
                  log.trace("ownableReentrantLock = " + ownableReentrantLock);
                  if (ownableReentrantLock != null) return;
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
         }
      }, false);

      Thread.sleep(200);
      lc.releaseLock(owner, k1);

      Thread.sleep(4000);
   }

   private String ab2() {
      return "ab";
   }

   public String ab() {
      StringBuilder sb = new StringBuilder("a");
      return sb.append("b").toString();
   }
}
