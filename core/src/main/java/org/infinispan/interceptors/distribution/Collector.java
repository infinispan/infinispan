package org.infinispan.interceptors.distribution;

import java.util.concurrent.CompletableFuture;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface Collector<T> {

   CompletableFuture<T> getFuture();

   void primaryException(Throwable throwable);

   void primaryResult(T result, boolean success);

}
