package org.infinispan.commons.util.concurrent;

/**
 * An abstract NotifyingFuture that has "completed"
 *
 * @author Manik Surtani
 * @version 4.1
 */
public abstract class AbstractInProcessNotifyingFuture<V> extends AbstractInProcessFuture<V> implements NotifyingFuture<V> {
   @Override
   public NotifyingFuture<V> attachListener(FutureListener<V> futureListener) {
      futureListener.futureDone(this);
      return this;
   }
}
