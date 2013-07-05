package org.infinispan.commons.util.concurrent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class BaseNotifyingFuture<T> implements NotifyingFuture<T> {

   final Set<FutureListener<T>> listeners = new CopyOnWriteArraySet<FutureListener<T>>();
   volatile boolean callCompleted = false;
   final ReadWriteLock listenerLock = new ReentrantReadWriteLock();


   public final NotifyingFuture<T> attachListener(FutureListener<T> objectFutureListener) {
      listenerLock.readLock().lock();
      try {
         if (!callCompleted) listeners.add(objectFutureListener);
         if (callCompleted) objectFutureListener.futureDone(this);
         return this;
      } finally {
         listenerLock.readLock().unlock();
      }
   }

   public void notifyDone() {
      listenerLock.writeLock().lock();
      try {
         callCompleted = true;
         for (FutureListener<T> l : listeners) l.futureDone(this);
      } finally {
         listenerLock.writeLock().unlock();
      }
   }
}
