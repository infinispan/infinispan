package org.infinispan.remoting.transport;

import org.infinispan.commons.util.Experimental;
import org.infinispan.remoting.responses.Response;

/**
 * A representation of a request's responses.
 *
 * <p>Thread-safety: The request will invoke {@link #addResponse(S, Response)} and
 * {@link #finish()} in a mutually exclusive way. If {@link #addResponse(S, Response)} is invoked more than once it may
 * be on different threads, but never concurrently and only with a happens-before operation ensuring proper visibility,
 * allowing implementations to use non concurrent structures.</p>
 *
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public interface ResponseCollector<S, T> {
   /**
    * Called when a response is received, or when a target node becomes unavailable.
    *
    * <p>When a target node leaves the cluster, this method is called with a
    * {@link org.infinispan.remoting.responses.CacheNotFoundResponse}.</p>
    *
    * <p>Should return a non-{@code null} result if the request should complete with that value, or {@code null}
    * if it should wait for more responses. If the method throws an exception, the request will be completed with that
    * exception.
    * <p>
    * If the last response is received and {@code addResponse()} still returns {@code null}, {@link #finish()} will also
    * be called to obtain a result.
    *
    * <p>Thread safety: {@code addResponse()} will *not* be called concurrently from multiple threads,
    * and the request will not be completed while {@code addResponse()} is running.</p>
    */
   T addResponse(S sender, Response response);

   /**
    * Called after {@link #addResponse(S, Response)} returns {@code null} for the last response.
    *
    * <p>If {@code finish()} finishes normally, the request will complete with its return value
    * (even if {@code null}). If {@code finish()} throws an exception, the request will complete exceptionally with that
    * exception, wrapped in a {@link java.util.concurrent.CompletionException} (unless the exception is already a
    * {@link java.util.concurrent.CompletionException}).
    * </p>
    */
   T finish();
}
