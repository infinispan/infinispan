package org.infinispan.stream.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.infinispan.Cache;

/**
 * An Iterator implementation that allows for a Iterator that doesn't allow remove operations to
 * implement remove by delegating the call to the provided cache to remove the previously read value.  The key used
 * to remove from the cache is determined by first applying the removeFunction to the value retrieved from the
 * iterator.
 *
 * @author wburns
 * @since 8.0
 * @deprecated Users should use {@link org.infinispan.commons.util.RemovableIterator} instead
 */
public class RemovableIterator<K, C> extends org.infinispan.commons.util.RemovableIterator<C> {
   public RemovableIterator(Iterator<C> realIterator, Cache<K, ?> cache,
           Function<? super C, K> removeFunction) {
      super(realIterator, c -> cache.remove(removeFunction.apply(c)));
   }
}
