package org.infinispan.commons.util.concurrent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Constructs an instance of a {@link org.infinispan.util.concurrent.NotifyingFuture}.
 * <p/>
 * Typical usage:
 * <p/>
 * <code> Object retval = .... // do some work here NotifyingFuture nf = new NotifyingFutureImpl(retval);
 * rpcManager.broadcastRpcCommandInFuture(nf, command); return nf; </code>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NotifyingFutureImpl<T> extends BaseNotifyingFuture<T> implements NotifyingNotifiableFuture<T>{

   final T actualReturnValue;
   volatile Future<T> ioFuture;
   //TODO revisit if volatile needed

   public NotifyingFutureImpl(T actualReturnValue) {
      this.actualReturnValue = actualReturnValue;
   }

   @Override
   public void setNetworkFuture(Future<T> future) {
      this.ioFuture = future;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return ioFuture.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return ioFuture.isCancelled();
   }

   @Override
   public boolean isDone() {
      return ioFuture.isDone();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      if (!callCompleted) {
         ioFuture.get();
      }
      return actualReturnValue;
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      if (!callCompleted) {
         ioFuture.get(timeout, unit);
      }
      return actualReturnValue;
   }
}