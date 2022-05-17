package org.infinispan.api.common;

import java.util.function.Consumer;

/**
 * Interface that provides semantics of a {@link Iterable} but produces {@link CloseableIterator} instances.
 * Note that the iterators produced via {@link #iterator()} do not need to be closed if fully iterated upon.
 * Therefore, methods like {@link Iterable#forEach(Consumer)} and {@link java.util.Iterator#forEachRemaining(Consumer)}
 * can be used without any worry to closing any iterators.
 *
 * @since 14.0
 */
public interface CloseableIterable<E> extends Iterable<E> {
   @Override
   CloseableIterator<E> iterator();
}
