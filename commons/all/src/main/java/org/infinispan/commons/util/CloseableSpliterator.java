package org.infinispan.commons.util;

import java.util.Spliterator;

/**
 * Interface that provides semantics of a {@link Spliterator} and {@link AutoCloseable} interfaces.  This is
 * useful when you have data that can be splitted and may hold resources in the underlying implementation that
 * must be closed.
 * <p>A spliterator split from this is not closeable.  Only the original {@link CloseableSpliterator} is
 * required to be closed</p>
 * <p>Some implementations may close resources automatically when the spliterator is exhausted however
 * this is an implementation detail and all callers should call {@link AutoCloseable#close()} method to be
 * sure all resources are freed properly.</p>
 * @since 8.0
 */
public interface CloseableSpliterator<T> extends Spliterator<T>, AutoCloseable {
   @Override
   void close();
}
