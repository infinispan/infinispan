package org.infinispan.util.concurrent;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
public class NotifyingFutureImpl implements NotifyingNotifiableFuture<Object> {

   final Object actualReturnValue;
   volatile Future<Object> ioFuture;
   volatile boolean callCompleted = false;
   final Set<FutureListener<Object>> listeners = new CopyOnWriteArraySet<FutureListener<Object>>();

   public NotifyingFutureImpl(Object actualReturnValue) {
      this.actualReturnValue = actualReturnValue;
   }

   public void setNetworkFuture(Future<Object> future) {
      this.ioFuture = future;
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      return ioFuture.cancel(mayInterruptIfRunning);
   }

   public boolean isCancelled() {
      return ioFuture.isCancelled();
   }

   public boolean isDone() {
      return ioFuture.isDone();
   }

   public Object get() throws InterruptedException, ExecutionException {
      ioFuture.get();
      return actualReturnValue;
   }

   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      ioFuture.get(timeout, unit);
      return actualReturnValue;
   }

   public void notifyDone() {
      callCompleted = true;
      for (FutureListener<Object> l : listeners) l.futureDone(this);
   }

   public NotifyingFuture<Object> attachListener(FutureListener<Object> objectFutureListener) {
      if (!callCompleted) listeners.add(objectFutureListener);
      if (callCompleted) objectFutureListener.futureDone(this);
      return this;
   }
}