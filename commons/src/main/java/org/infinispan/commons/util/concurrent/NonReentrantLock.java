package org.infinispan.commons.util.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simplistic non-reentrant lock that does not use ownership by thread.
 */
public final class NonReentrantLock implements Lock {
   private final Lock inner = new ReentrantLock();
   private Condition condition;

   @Override
   public synchronized void lock() {
      inner.lock();
      try {
         if (condition == null) {
            condition = inner.newCondition();
         } else {
            condition.awaitUninterruptibly();
         }
      } finally {
         inner.unlock();
      }
   }

   @Override
   public void lockInterruptibly() throws InterruptedException {
      inner.lockInterruptibly();
      try {
         if (condition == null) {
            condition = inner.newCondition();
         } else {
            condition.await();
         }
      } finally {
         inner.unlock();
      }
   }

   @Override
   public boolean tryLock() {
      if (inner.tryLock()) {
         try {
            if (condition == null) {
               condition = inner.newCondition();
               return true;
            }
         } finally {
            inner.unlock();
         }
      }
      return false;
   }

   @Override
   public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      long deadline = System.currentTimeMillis() + unit.toMillis(time);
      if (inner.tryLock(time, unit)) {
         try {
            while (condition != null) {
               long now = System.currentTimeMillis();
               if (now >= deadline) {
                  return false;
               }
               condition.await(deadline - now, TimeUnit.MILLISECONDS);
               return true;
            }
            condition = inner.newCondition();
         } finally {
            inner.unlock();
         }
      }
      return false;
   }

   @Override
   public void unlock() {
      inner.lock();
      try {
         condition.signalAll();
         condition = null;
      } finally {
         inner.unlock();
      }
   }

   @Override
   public Condition newCondition() {
      throw new UnsupportedOperationException();
   }
}
