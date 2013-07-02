package org.infinispan.util.concurrent;

import java.util.concurrent.Future;

/**
 * A sub-interface of a Future, that allows for listeners to be attached so that observers can be notified of when the
 * future completes.
 * <p/>
 * See {@link FutureListener} for more details.
 * <p/>
 * {@link #attachListener(FutureListener)} returns the same future instance, which is useful for 'building' a future.
 * E.g.,
 * <p/>
 * <code> Future<Void> f = cache.clearAsync().attachListener(new MyCustomListener()); </code>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface NotifyingFuture<T> extends Future<T> {

   /**
    * Attaches a listener and returns the same future instance, to allow for 'building'.
    *
    * @param listener listener to attach
    * @return the same future instance
    */
   NotifyingFuture<T> attachListener(FutureListener<T> listener);

}
