package org.infinispan.commons.util.concurrent;

import org.infinispan.commons.CacheException;

/**
 * A {@link CacheException} that is thrown when the backpressure has been filled an unable to process the request. This
 * can be thrown during periods where too much concurrency was requested or the load from an external source is too
 * high. This can be remedied by reducing the load or increasing the backpressure handling (usually buffering of some sort).
 */
public class CacheBackpressureFullException extends CacheException {
}
