package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.Set;

/**
 * A set that allows reverse iteration of the set elements, exposed via the {@link #reverseIterator()} method.  This
 * only really makes sense for ordered Set implementations, such as sets which are linked.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ReversibleOrderedSet<E> extends Set<E> {
   Iterator<E> reverseIterator();
}
