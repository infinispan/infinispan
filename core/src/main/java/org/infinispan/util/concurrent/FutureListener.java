package org.infinispan.util.concurrent;

import java.util.concurrent.Future;

/**
 * A listener that is called back when a future is done.  FutureListener instances are attached to {@link
 * NotifyingFuture}s by passing them in to {@link NotifyingFuture#attachListener(FutureListener)}
 * <p/>
 * Note that the {@link #futureDone(Future)} callback is invoked when the future completes, regardless of how the future
 * completes (i.e., normally, due to an exception, or cancelled}.  As such, implementations should check the future
 * passed in by calling <tt>future.get()</tt>.
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Use {@link org.infinispan.commons.util.concurrent.FutureListener} instead
 */
@Deprecated
public interface FutureListener<T> extends org.infinispan.commons.util.concurrent.FutureListener<T> {
}
