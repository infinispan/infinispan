package org.infinispan.lock;

import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "lock.SimpleLockContainerTest")
public class SimpleLockContainerTest extends AbstractInfinispanTest {

   PerKeyLockContainer lc = new PerKeyLockContainer();

   public void simpleTest() throws Exception {
      lc.inject(ForkJoinPool.commonPool(), TIME_SERVICE);
      final String k1 = ab();
      final String k2 = ab2();
      assert k1 != k2 && k1.equals(k2);

      Object owner = new Object();
      lc.acquire(k1, owner, 0, TimeUnit.MILLISECONDS).lock();
      assert lc.getLock(k1).isLocked();


      Future<Void> f = fork(new Callable<Void>() {
         @Override
         public Void call() throws InterruptedException, TimeoutException {
            final Object otherOwner = new Object();
            for (int i =0; i < 10; i++) {
               try {
                  lc.acquire(k2, otherOwner, 500, TimeUnit.MILLISECONDS).lock();
                  return null;
               } catch (TimeoutException e) {
                  //ignored
               }
            }
            throw new TimeoutException("We should have acquired lock!");
         }
      });

      Thread.sleep(200);
      lc.release(k1, owner);

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
