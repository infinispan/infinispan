package org.infinispan.lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.locks.OwnableReentrantLock;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Test (groups = "functional", testName = "lock.SimpleLockContainerTest")
public class SimpleLockContainerTest extends AbstractInfinispanTest {

   OwnableReentrantPerEntryLockContainer lc = new OwnableReentrantPerEntryLockContainer(1000, AnyEquivalence.getInstance());

   public void simpleTest() throws Exception {
      final String k1 = ab();
      final String k2 = ab2();
      assert k1 != k2 && k1.equals(k2);

      Object owner = new Object();
      lc.acquireLock(owner, k1, 0, TimeUnit.MILLISECONDS);
      assert lc.isLocked(k1);


      Future<Void> f = fork(new Callable<Void>() {
         @Override
         public Void call() throws InterruptedException, TimeoutException {
            final Object otherOwner = new Object();
            for (int i =0; i < 10; i++) {
               final OwnableReentrantLock ownableReentrantLock = lc.acquireLock(otherOwner, k2, 500, TimeUnit.MILLISECONDS);
               log.trace("ownableReentrantLock = " + ownableReentrantLock);
               if (ownableReentrantLock != null) return null;
            }
            throw new TimeoutException("We should have acquired lock!");
         }
      });

      Thread.sleep(200);
      lc.releaseLock(owner, k1);

      f.get(10, TimeUnit.SECONDS);
   }

   private String ab2() {
      return "ab";
   }

   public String ab() {
      StringBuilder sb = new StringBuilder("a");
      return sb.append("b").toString();
   }
}
