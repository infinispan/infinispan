package org.infinispan.commons.util;

import java.util.Set;

/**
 * A set that defines an iterator method that returns a {@link org.infinispan.commons.util.CloseableIterator}
 * instead of a non closeable one.  This is needed so that iterators can be properly cleaned up.  All other
 * methods will internally clean up any iterators created and don't have other side effects.
 *
 * @author wburns
 * @since 7.0
 */
public interface CloseableIteratorSet<E> extends Set<E>, CloseableIteratorCollection<E> {
}
