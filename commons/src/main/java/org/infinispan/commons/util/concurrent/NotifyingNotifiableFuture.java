package org.infinispan.commons.util.concurrent;

import java.util.concurrent.Future;

/**
 * An internal interface which adds the ability to inform the future of completion.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface NotifyingNotifiableFuture<T> extends NotifyingFuture<T> {
   /**
    * Notify the listeners that the operation has finished. Subsequent calls for isDone()
    * will return true and subsequent calls for get() will return the provided result.
    * @param result
    */
   void notifyDone(T result);

   /**
    * Notify the listeners that the operation has finished. Subsequent calls for isDone()
    * will return true and subsequent calls for get() will throw the provided exception.
    * @param exception
    */
   void notifyException(Throwable exception);

   /**
    * Setup the future which is wrapped by implementation of this interface.
    *
    * Warning: the implementation must synchronize the call setFuture()
    * and notifyDone() to be executed in this order. Also, all calls delegated to the
    * future must be deferred until the network future is set up.
    * @param future
    */
   void setFuture(Future<T> future);
}
